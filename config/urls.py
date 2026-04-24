from django.contrib import admin
from django.urls import path
from django.http import JsonResponse

def home(request):
    return JsonResponse({
        'message': 'API is running',
        'status': 'ok'
    })

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', home),
]
