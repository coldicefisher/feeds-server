"""
WSGI config for django_trader project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_trader.settings')

application = get_wsgi_application()

# Custom startup code /////////////////////////////////////////////////////////
import django_trader.startup as startup
startup.run()
# End custom startup code ////////////////////////////////////////////////////