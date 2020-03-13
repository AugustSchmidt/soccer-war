
from django.urls import path
from . import views
from search.views import MainView

urlpatterns = [
    path('', MainView.as_view(), name='home'),
]
