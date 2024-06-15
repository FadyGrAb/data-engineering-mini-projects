c = get_config()

c.NotebookApp.ip = '0.0.0.0'
c.NotebookApp.port = 8888
c.NotebookApp.open_browser = False

from notebook.auth import passwd
c.NotebookApp.password = passwd('notebookpassword')

# Uncomment and set a base URL path for added security
# c.NotebookApp.base_url = '/jupyter'

# Uncomment and set the following to use HTTPS (recommended for remote access)
# c.NotebookApp.certfile = u'/path/to/your/cert.pem'
# c.NotebookApp.keyfile = u'/path/to/your/key.pem'

# Set the working directory
# c.NotebookApp.notebook_dir = '/path/to/your/notebooks'

# # Enabling Jupyter extensions
# c.NotebookApp.nbserver_extensions = {
#     'jupyter_http_over_ws': True,
# }

# # Additional performance and security settings
# c.NotebookApp.iopub_data_rate_limit = 10000000
# c.NotebookApp.allow_root = True
# c.NotebookApp.disable_check_xsrf = False
