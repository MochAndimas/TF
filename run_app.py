"""Run App module.

This module is part of `.` and contains runtime logic used by the
Traders Family application.
"""

import subprocess
subprocess.run(["streamlit","run","streamlit_run.py","--server.port","5504"])