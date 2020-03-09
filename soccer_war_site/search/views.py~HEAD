from django.shortcuts import render, render_to_response

import json
import traceback
import sys
import csv
import os
from django.shortcuts import render
from django import forms
from django.db.models import Q
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
            if not 1 <= v <= 100:
                raise forms.ValidationError(
                    'Enrollment bounds must be in the range 1 to 100.')
        if data_list and (data_list[1] < data_list[0]):
            raise forms.ValidationError(
                'Lower bound must not exceed upper bound.')
        return data_list

class SearchForm(forms.Form):
    query = forms.CharField(
        label='Player',
        help_text='e.g. Harry Kane',
        required=False)
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
    # time_and_building = BuildingWalkingTime(
    #     label='Walking time:',
    #     help_text='e.g. 10 and RY (at most a 10-min walk from Ryerson)',
    #     required=False,
    #     widget=forms.widgets.MultiWidget(
    #         widgets=(forms.widgets.NumberInput,
    #                  forms.widgets.Select(choices=BUILDINGS))))
    position = forms.ChoiceField(label='Position', choices=POSITIONS, required=False)
    squads = forms.MultipleChoiceField(label='Teams',
                                     choices=SQUADS,
                                     widget=forms.CheckboxSelectMultiple,
                                     required=False) 
    show_args = forms.BooleanField(label='Show args_to_ui',
                                   required=False)

def home(request):
    context = {}
    res = None
    if request.method == 'GET':
        # create a form instance and populate it with data from the request:
        form = SearchForm(request.GET)
        # check whether it's valid:
        if form.is_valid():
            # Convert form data to an args dictionary for find_courses
            args = {}
            if form.cleaned_data['query']:
                args['Player'] = form.cleaned_data['query']
            age = form.cleaned_data['age']
            if age:
                args['age_lower'] = age[0]
                args['age_upper'] = age[1]
            gls = form.cleaned_data['gls']
            if gls:
                args['gls_lower'] = gls[0]
                args['gls_upper'] = gls[1]
            positions = form.cleaned_data['position']
            if positions:
                args['positions'] = positions
            squads = form.cleaned_data['squads']
            if squads:
                args['squads'] = squads
            if form.cleaned_data['show_args']:
                context['args'] = 'args_to_ui = ' + json.dumps(args, indent=2)
            try:
                print('args:', args)
                res = find_players(args)
                print('res:', res)
            except Exception as e:
                print('Exception caught')
                bt = traceback.format_exception(*sys.exc_info()[:3])
                context['err'] = """
                An exception was thrown in find_courses:
                <pre>{}
                {}</pre>
                """.format(e, '\n'.join(bt))
                res = None

    else:
        form = SearchForm()

    # Handle different responses of res
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

        # Wrap in tuple if result is not already
        if result and isinstance(result[0], str):
            result = [(r,) for r in result]

        context['result'] = result
        context['num_results'] = len(result)
        context['columns'] = [COLUMN_NAMES.get(col, col) for col in columns]

    context['form'] = form
    return render(request, 'index.html', context= context)


# def index(request):
#   return render_to_response('index.html')
