# =============================================================================
# APACHE AIRFLOW - WEBSERVER CONFIGURATION
# =============================================================================
# Configuração do Flask AppBuilder para Airflow Webserver
# =============================================================================

import os
from airflow.www.fab_security.manager import AUTH_DB
# from airflow.www.fab_security.manager import AUTH_LDAP
# from airflow.www.fab_security.manager import AUTH_OAUTH
# from airflow.www.fab_security.manager import AUTH_OID
# from airflow.www.fab_security.manager import AUTH_REMOTE_USER

basedir = os.path.abspath(os.path.dirname(__file__))

# =============================================================================
# FLASK APP CONFIG
# =============================================================================

# Flask secret key
SECRET_KEY = os.environ.get('AIRFLOW_SECRET_KEY', 'your-secret-key-here')

# =============================================================================
# AUTHENTICATION
# =============================================================================

# Authentication type
# AUTH_DB = Database authentication
# AUTH_LDAP = LDAP authentication
# AUTH_OAUTH = OAuth authentication
# AUTH_OID = OpenID authentication
# AUTH_REMOTE_USER = Remote user authentication
AUTH_TYPE = AUTH_DB

# Uncommit to setup Full admin role name
# AUTH_ROLE_ADMIN = 'Admin'

# Uncommit to setup Public role name, no authentication needed
# AUTH_ROLE_PUBLIC = 'Public'

# Will allow user self registration
AUTH_USER_REGISTRATION = True

# The default user self registration role
AUTH_USER_REGISTRATION_ROLE = "Public"

# =============================================================================
# ROLES CONFIGURATION
# =============================================================================

# Role mapping for self registration
# When using AUTH_DB, new users will be assigned the role specified in AUTH_USER_REGISTRATION_ROLE
# When using OAuth or other methods, you can map roles from the provider
AUTH_ROLES_MAPPING = {
    "airflow_admin": ["Admin"],
    "airflow_op": ["Op"],
    "airflow_user": ["User"],
    "airflow_viewer": ["Viewer"],
}

# Sync roles at login
AUTH_ROLES_SYNC_AT_LOGIN = True

# =============================================================================
# LDAP CONFIGURATION (se usar AUTH_LDAP)
# =============================================================================

# AUTH_LDAP_SERVER = "ldap://ldapserver.new"
# AUTH_LDAP_USE_TLS = False
# AUTH_LDAP_ALLOW_SELF_SIGNED = True
# AUTH_LDAP_BIND_USER = "cn=admin,dc=example,dc=com"
# AUTH_LDAP_BIND_PASSWORD = "password"
# AUTH_LDAP_SEARCH = "ou=users,dc=example,dc=com"
# AUTH_LDAP_UID_FIELD = "uid"
# AUTH_LDAP_FIRSTNAME_FIELD = "givenName"
# AUTH_LDAP_LASTNAME_FIELD = "sn"
# AUTH_LDAP_EMAIL_FIELD = "mail"

# =============================================================================
# OAUTH CONFIGURATION (se usar AUTH_OAUTH)
# =============================================================================

# OAUTH_PROVIDERS = [
#     {
#         'name': 'google',
#         'icon': 'fa-google',
#         'token_key': 'access_token',
#         'remote_app': {
#             'client_id': 'GOOGLE_CLIENT_ID',
#             'client_secret': 'GOOGLE_CLIENT_SECRET',
#             'api_base_url': 'https://www.googleapis.com/oauth2/v2/',
#             'client_kwargs': {
#                 'scope': 'email profile'
#             },
#             'access_token_url': 'https://accounts.google.com/o/oauth2/token',
#             'authorize_url': 'https://accounts.google.com/o/oauth2/auth',
#         }
#     }
# ]

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

# SQL Alchemy connection string (deve coincidir com airflow.cfg)
SQLALCHEMY_DATABASE_URI = os.environ.get(
    'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
    'postgresql+psycopg2://airflow:airflow123@postgres:5432/airflow'
)

# =============================================================================
# WEBSERVER CONFIGURATION
# =============================================================================

# The base URL of your website
WTF_CSRF_ENABLED = True

# CSRF token timeout (seconds)
WTF_CSRF_TIME_LIMIT = None

# Uncomment to setup Setup Session timeout (in minutes)
PERMANENT_SESSION_LIFETIME = 43200  # 12 hours

# =============================================================================
# THEME CONFIGURATION
# =============================================================================

# Flask-AppBuilder supports multiple themes
# APP_THEME = "bootstrap-theme.css"  # default
# APP_THEME = "cerulean.css"
# APP_THEME = "cosmo.css"
# APP_THEME = "cyborg.css"
# APP_THEME = "darkly.css"
# APP_THEME = "flatly.css"
# APP_THEME = "journal.css"
# APP_THEME = "lumen.css"
# APP_THEME = "paper.css"
# APP_THEME = "readable.css"
# APP_THEME = "sandstone.css"
# APP_THEME = "simplex.css"
# APP_THEME = "slate.css"
# APP_THEME = "spacelab.css"
# APP_THEME = "superhero.css"
# APP_THEME = "united.css"
# APP_THEME = "yeti.css"

# =============================================================================
# RATE LIMITING
# =============================================================================

# Rate limiting configuration
# RATELIMIT_ENABLED = True
# RATELIMIT_STORAGE_URL = "redis://redis:6379/1"

# =============================================================================
# LOGGING
# =============================================================================

# Console Log formatting
LOG_FORMAT = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"
LOG_LEVEL = "INFO"

# Enable Logging
ENABLE_PROXY_FIX = True

# =============================================================================
# SECURITY
# =============================================================================

# Security configuration
# Set to True to enable HTTPS
# ENABLE_HTTPS = False

# Cookie security
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # Set to True if using HTTPS
SESSION_COOKIE_SAMESITE = "Lax"

# CSRF configuration
WTF_CSRF_METHODS = ["POST", "PUT", "PATCH", "DELETE"]

# =============================================================================
# UI CONFIGURATION
# =============================================================================

# Number of records per page
PAGE_SIZE = 20

# UI configuration
APP_NAME = "SPTrans Pipeline"
APP_ICON = "/static/pin_100.png"
APP_ICON_WIDTH = 35

# =============================================================================
# INTERNATIONALIZATION
# =============================================================================

# Babel configuration
BABEL_DEFAULT_LOCALE = "pt_BR"
BABEL_DEFAULT_FOLDER = "translations"

# Languages available
LANGUAGES = {
    "en": {"flag": "gb", "name": "English"},
    "pt": {"flag": "br", "name": "Portuguese"},
    "pt_BR": {"flag": "br", "name": "Portuguese (Brazil)"},
    "es": {"flag": "es", "name": "Spanish"},
}

# =============================================================================
# CUSTOM CONFIGURATION
# =============================================================================

# Color theme for navbar
NAVBAR_COLOR = "#007A87"

# =============================================================================
# API CONFIGURATION
# =============================================================================

# API configuration
# Maximum number of items returned in a single API response
API_PAGE_SIZE = 100

# =============================================================================
# EMAIL CONFIGURATION (para alertas)
# =============================================================================

# Email configuration
# MAIL_SERVER = 'smtp.gmail.com'
# MAIL_PORT = 587
# MAIL_USE_TLS = True
# MAIL_USE_SSL = False
# MAIL_USERNAME = 'your-email@gmail.com'
# MAIL_PASSWORD = 'your-app-password'
# MAIL_DEFAULT_SENDER = 'airflow@sptrans-pipeline.com'

# =============================================================================
# FAB CONFIGURATION
# =============================================================================

# FAB API configuration
FAB_API_SWAGGER_UI = True
FAB_API_MAX_PAGE_SIZE = 100

# =============================================================================
# CUSTOM SECURITY MANAGER (opcional)
# =============================================================================

# from custom_security_manager import CustomSecurityManager
# CUSTOM_SECURITY_MANAGER = CustomSecurityManager

# =============================================================================
# ADDITIONAL LINKS (no menu)
# =============================================================================

# Adicionar links customizados no menu
# from airflow.www.utils import UIAlert
# ADDITIONAL_NAV_ITEMS = [
#     {
#         'name': 'Documentation',
#         'href': 'https://docs.sptrans-pipeline.com',
#         'target': '_blank',
#     },
#     {
#         'name': 'Grafana',
#         'href': 'http://localhost:3000',
#         'target': '_blank',
#     },
#     {
#         'name': 'Superset',
#         'href': 'http://localhost:8088',
#         'target': '_blank',
#     },
# ]

# =============================================================================
# BLUEPRINTS (custom views - opcional)
# =============================================================================

# from airflow.www.blueprints import routes
# ADDITIONAL_BLUEPRINTS = [routes]

# =============================================================================
# PLUGINS (FAB plugins - opcional)
# =============================================================================

# PLUGINS = ['PluginClass']

# =============================================================================
# DEVELOPMENT SETTINGS
# =============================================================================

# Debug mode (NUNCA use em produção!)
DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'

# Profiling (para debugging de performance)
# PROFILING = False
# PROFILING_SORT = 'cumulative'
# PROFILING_RESTRICTIONS = [30]

# =============================================================================
# HEALTHCHECK
# =============================================================================

# Healthcheck endpoint configuration
# HEALTHCHECK_ENDPOINT = '/health'

# =============================================================================
# CORS (Cross-Origin Resource Sharing)
# =============================================================================

# Enable CORS
# ENABLE_CORS = False
# CORS_ORIGINS = ['http://localhost:3000']

# =============================================================================
# CUSTOM CSS/JS (opcional)
# =============================================================================

# Path to custom CSS
# CUSTOM_CSS = '/static/custom.css'

# Path to custom JS
# CUSTOM_JS = '/static/custom.js'

# =============================================================================
# ADVANCED CONFIGURATION
# =============================================================================

# SQL Lab configuration (se usar SQL Lab)
# SQLLAB_TIMEOUT = 300
# SQLLAB_QUERY_COST_ESTIMATE_TIMEOUT = 10

# Cache configuration
# CACHE_CONFIG = {
#     'CACHE_TYPE': 'redis',
#     'CACHE_REDIS_HOST': 'redis',
#     'CACHE_REDIS_PORT': 6379,
#     'CACHE_REDIS_DB': 1,
#     'CACHE_DEFAULT_TIMEOUT': 300
# }

# =============================================================================
# CALLBACK CONFIGURATION
# =============================================================================

# Callbacks para eventos específicos
# def on_login(user):
#     """Callback executado quando usuário faz login"""
#     print(f"User {user.username} logged in")
#
# LOGIN_CALLBACK = on_login

# =============================================================================
# NOTES
# =============================================================================

# Para gerar SECRET_KEY:
#   python -c "import secrets; print(secrets.token_hex(32))"
#
# Para criar usuário admin via CLI:
#   airflow users create \
#     --username admin \
#     --firstname Admin \
#     --lastname User \
#     --role Admin \
#     --email admin@sptrans.com \
#     --password admin
#
# Para resetar senha:
#   airflow users reset-password --username admin

# =============================================================================
# END
# =============================================================================
