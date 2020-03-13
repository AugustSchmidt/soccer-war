from django.shortcuts import render, render_to_response

import json
import traceback
import sys
import csv
import os
from django.shortcuts import render
from django import forms
from django.db.models import Q
from django.views.generic.base import TemplateView
# from search import models
from player_info import find_players
# from search import searchplayers


NOPREF_STR = 'No preference'
RES_DIR = os.path.join(os.path.dirname(__file__), '..', 'res')
COLUMN_NAMES = dict(age= 'Age', gls= 'Gls', pos= 'Pos', squad= 'Squad')

# next thing to do is add load_columns method
# need to add file in res folder that will get distinct values
# for columns 

RANGE_WIDGET = forms.widgets.MultiWidget(widgets=(forms.widgets.NumberInput,
                                                  forms.widgets.NumberInput))

def _valid_result(res):
    """Validate results returned by find_players"""
    (HEADER, RESULTS) = [0, 1]
    ok = (isinstance(res, (tuple, list)) and
          len(res) == 2 and
          isinstance(res[HEADER], (tuple, list)) and
          isinstance(res[RESULTS], (tuple, list)))
    if not ok:
        return False
    else:
        return True


def load_column(filename, col=0):
    """Load single column from csv file."""
    with open(filename) as f:
        col = list(csv.reader(f))
        col = [item for val in col for item in val]
        return col

def load_res_column(filename, col=0):
    """Load column from resource directory."""
    return load_column(os.path.join(RES_DIR, filename), col=col)

def build_dropdown(options):
    """Convert a list to (value, caption) tuples."""
    return [(x, x) if x is not None else ('', NOPREF_STR) for x in options]

SQUADS = build_dropdown(load_res_column('squad_list.csv'))
POSITIONS = build_dropdown(load_res_column('pos_list.csv'))
NATIONS = build_dropdown(load_res_column('nation_list.csv'))
POSITIONS.insert(0, ('Wing Back', 'Wing Back'))
POSITIONS.insert(0, ('Winger', 'Winger'))
NATIONS.insert(0, ('All', 'All'))
year = 1992
tables = []
while year <= 2019:
    s = str(year) + '-' + str(year + 1)
    tables.append(s)
    year += 1
tables.insert(0, 'All')
SEASONS = build_dropdown(tables)


class IntegerRange(forms.MultiValueField):
    def __init__(self, *args, **kwargs):
        fields = (forms.IntegerField(),
                  forms.IntegerField())
        super(IntegerRange, self).__init__(fields=fields,
                                           *args, **kwargs)

    def compress(self, data_list):
        if data_list and (data_list[0] is None or data_list[1] is None):
            raise forms.ValidationError('Must specify both lower and upper '
                                        'bound, or leave both blank.')

        return data_list

class value_range(IntegerRange):
    def compress(self, data_list):
        super(value_range, self).compress(data_list)
        for v in data_list:
            if not 0 <= v <= 100:
                raise forms.ValidationError(
                    'Integer bounds must be in the range 0 to 100.')
        if data_list and (data_list[1] < data_list[0]):
            raise forms.ValidationError(
                'Lower bound must not exceed upper bound.')
        return data_list

class PosForm(forms.Form):
    position = forms.ChoiceField(label='Position', choices=POSITIONS, required=False)

class FieldPlayerForm(forms.Form):
    options = ['None', 'Season', 'Age', 'Goals', 'Assists', 
                'Nationality', 'Squad']
    FIELDS = build_dropdown(options)

    query = forms.CharField(
        label='Player',
        help_text='e.g. Harry Kane',
        required=False)
    season = forms.ChoiceField(label='Season', choices = SEASONS, required = False)
    age = value_range(
        label='Age',
        help_text='e.g. 20 and 26',
        widget=RANGE_WIDGET,
        required=False)
    gls = value_range(
        label='Goals',
        help_text='e.g. 5 and 15',
        widget=RANGE_WIDGET,
        required=False)
    ast = value_range(
        label='Assists',
        help_text='e.g. 5 and 15',
        widget=RANGE_WIDGET,
        required=False)
    # position = forms.ChoiceField(label='Position', choices=POSITIONS, required=False)
    nation = forms.ChoiceField(label='Nationality', choices=NATIONS, required=False)
    squads = forms.MultipleChoiceField(label='Teams',
                                     choices=SQUADS,
                                     widget=forms.CheckboxSelectMultiple,
                                     required=False)
    order = forms.ChoiceField(label='Sort By', choices=FIELDS, required=False)
    show_args = forms.BooleanField(label='Show args_to_ui',
                                   required=False)

class GKForm(forms.Form):
        options = ['None', 'Season', 'Age', 'GA90', 'GA', 'Save%', 
                    'Clean Sheets', 'Nationality', 'Squad']
        FIELDS = build_dropdown(options)
        query = forms.CharField(
        label='Player',
        help_text='e.g. Harry Kane',
        required=False)
        season = forms.ChoiceField(label='Season', choices = SEASONS, required = False)
        age = value_range(
        label='Age',
        help_text='e.g. 20 and 26',
        widget=RANGE_WIDGET,
        required=False)
        ga90 = value_range(
            label = 'Goals Against per 90',
            help_text = 'e.g. 0.5 and 1.5',
            widget = RANGE_WIDGET,
            required = False)
        ga = value_range(
            label = 'Goals Against (total)',
            help_text = 'e.g. 15 and 30',
            widget = RANGE_WIDGET,
            required = False)
        save_per = value_range(
            label = 'Save Percentage:',
            help_text = 'e.g. 0.75 and 0.90',
            widget = RANGE_WIDGET,
            required = False)
        clean_sheets = value_range(
            label = 'Clean Sheets:',
            help_text = 'Games with no goals allowed, e.g. 15 and 25',
            widget = RANGE_WIDGET,
            required = False)
        nation = forms.ChoiceField(label='Nationality', choices=NATIONS, required=False)
        squads = forms.MultipleChoiceField(label='Teams',
                                     choices=SQUADS,
                                     widget=forms.CheckboxSelectMultiple,
                                     required=False)
        order = forms.ChoiceField(label='Sort By', choices=FIELDS, required=False)
        show_args = forms.BooleanField(label='Show args_to_ui',
                                   required=False)


class MainView(TemplateView):
    template_name = 'index.html'
    def get(self, request, *args, **kwargs):
        context = {}
        res = None
        pos = ''
        if request.method == 'GET':
            pos_form = PosForm(request.GET)
            if pos_form.is_valid():
                if pos_form.cleaned_data['position']:
                    pos = pos_form.cleaned_data['position']
        print('POS:', pos)
        if pos != 'GK':
            form = FieldPlayerForm(request.GET)
            if form.is_valid():
                # Convert form data to an args dictionary for find_courses
                args = {'Pos': pos}
                if form.cleaned_data['query']:
                    args['Player'] = form.cleaned_data['query']
                season = form.cleaned_data['season']
                if season:
                    args['season'] = season
                age = form.cleaned_data['age']
                if age:
                    args['age_lower'] = age[0]
                    args['age_upper'] = age[1]
                gls = form.cleaned_data['gls']
                if gls:
                    args['gls_lower'] = gls[0]
                    args['gls_upper'] = gls[1]
                ast = form.cleaned_data['ast']
                if ast:
                    args['Ast'] = (ast[0], ast[1])
                nation = form.cleaned_data['nation']
                if nation:
                    args['Nation'] = nation
                squads = form.cleaned_data['squads']
                if squads:
                    args['Squad'] = squads
                order = form.cleaned_data['order']
                if order:
                    args['order_by'] = order
                if form.cleaned_data['show_args']:
                    context['args'] = 'args_to_ui = ' + json.dumps(args, indent=2)
        else:
            form = GKForm(request.GET)
            if form.is_valid():
                args = {'Pos': pos}
                if form.cleaned_data['query']:
                    args['Player'] = form.cleaned_data['query']
                season = form.cleaned_data['season']
                if season:
                    args['season'] = season
                age = form.cleaned_data['age']
                if age:
                    args['age_lower'] = age[0]
                    args['age_upper'] = age[1]
                ga90 = form.cleaned_data['ga90']
                if ga90:
                    args['ga90_lower'] = ga90[0]
                    args['ga90_upper'] = ga90[1]
                ga = form.cleaned_data['ga']
                if ga:
                    args['ga_lower'] = ga[0]
                    args['ga_upper'] = ga[1]
                save_per = form.cleaned_data['save_per']
                if save_per:
                    args['save_per_lower'] = save_per[0]
                    args['save_per_upper'] = save_per[1]
                clean_sheets = form.cleaned_data['clean_sheets']
                if clean_sheets:
                    args['cs_lower'] = clean_sheets[0]
                    args['cs_upper'] = clean_sheets[1]
                nation = form.cleaned_data['nation']
                if nation:
                    args['Nation'] = nation
                squads = form.cleaned_data['squads']
                if squads:
                    args['Squad'] = squads
                order = form.cleaned_data['order']
                if order:
                    args['order_by'] = order
                if form.cleaned_data['show_args']:
                    context['args'] = 'args_to_ui = ' + json.dumps(args, indent=2)

        try:
            res = find_players(args)
        except Exception as e:
            print('Exception caught')
            bt = traceback.format_exception(*sys.exc_info()[:3])
            context['err'] = """
            An exception was thrown in find_courses:
            <pre>{}
            {}</pre>
            """.format(e, '\n'.join(bt))
            res = None
        if res is None:
            context['result'] = None
        elif isinstance(res, str):
            context['result'] = None
            context['err'] = res
            result = None
        elif not _valid_result(res):
            context['result'] = None
            context['err'] = ('Return of find_players has the wrong data type. '
                              'Should be a tuple of length 4 with one string and '
                              'three lists.')
        else:
            columns, result = res


        context = self.get_context_data(**kwargs)
        context['pos_form'] = pos_form
        context['form'] = form
        return render(request, 'index.html', context=context)

# class PosFormView(FormView):

# def home(request):
#     context = {}
#     res = None
#     if request.method == 'GET':
#         # create a form instance for first positional argument
#         pos_form = PosForm(request.GET)
#         if pos_form.is_valid():
#             pos = ''
#             if pos_form.cleaned_data['position']:
#                 pos = pos_form.cleaned_data['position']

#         # create a form instance and populate it with data from the request:
#         print('position:', pos)
#         if pos != '':
            # form = SearchForm(request.GET)
            # # check whether it's valid:
            # if form.is_valid():
            #     # Convert form data to an args dictionary for find_courses
            #     args = {}
            #     if form.cleaned_data['query']:
            #         args['Player'] = form.cleaned_data['query']
            #     season = form.cleaned_data['season']
            #     if season:
            #         args['season'] = season
            #     age = form.cleaned_data['age']
            #     if age:
            #         args['age_lower'] = age[0]
            #         args['age_upper'] = age[1]
            #     gls = form.cleaned_data['gls']
            #     if gls:
            #         args['gls_lower'] = gls[0]
            #         args['gls_upper'] = gls[1]
            #     ast = form.cleaned_data['ast']
            #     if ast:
            #         args['Ast'] = (ast[0], ast[1])
            #     # positions = form.cleaned_data['position']
            #     # if positions:
            #     #     args['Pos'] = positions
            #     nation = form.cleaned_data['nation']
            #     if nation:
            #         args['Nation'] = nation
            #     squads = form.cleaned_data['squads']
            #     if squads:
            #         args['Squad'] = squads
            #     order = form.cleaned_data['order']
            #     if order:
            #         args['order_by'] = order
            #     if form.cleaned_data['show_args']:
            #         context['args'] = 'args_to_ui = ' + json.dumps(args, indent=2)
            #     try:
            #         res = find_players(args)
            #     except Exception as e:
            #         print('Exception caught')
            #         bt = traceback.format_exception(*sys.exc_info()[:3])
            #         context['err'] = """
            #         An exception was thrown in find_courses:
            #         <pre>{}
            #         {}</pre>
            #         """.format(e, '\n'.join(bt))
            #         res = None

#     else:
#         pos_form = PosForm()

#     # Handle different responses of res
    # if res is None:
    #     context['result'] = None
    # elif isinstance(res, str):
    #     context['result'] = None
    #     context['err'] = res
    #     result = None
    # elif not _valid_result(res):
    #     context['result'] = None
    #     context['err'] = ('Return of find_players has the wrong data type. '
    #                       'Should be a tuple of length 4 with one string and '
    #                       'three lists.')
    # else:
    #     columns, result = res

#         # Wrap in tuple if result is not already
#         if result and isinstance(result[0], str):
#             result = [(r,) for r in result]

#         context['result'] = result
#         context['num_results'] = len(result)
#         context['columns'] = [COLUMN_NAMES.get(col, col) for col in columns]
#     if pos != '':
#         context['form'] = form
#     else:
#         context['pos_form'] = pos_form
#     print('REQUEST:', request)
#     print('CONTEXT:', context)
#     return render(request, 'index.html', context= context)

