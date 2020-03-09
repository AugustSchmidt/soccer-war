"""soccer_war_site URL Configuration
"""
from django.contrib import admin
from django.urls import path
from django.conf.urls import url, include
from search import views as v

urlpatterns = [
    path('', include('search.urls')),
]
