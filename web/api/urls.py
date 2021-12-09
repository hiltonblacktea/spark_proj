from django.urls import path , include
from . import views

urlpatterns = [
    path('getData',views.get_data.as_view(),name='getData')
]