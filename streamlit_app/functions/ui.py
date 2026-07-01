"""Small reusable Streamlit UI fragments."""

from __future__ import annotations

from datetime import datetime


def footer(st_module):
    """Render fixed footer element at the bottom of Streamlit page."""
    footer_html = f"""
    <style>
        .footer {{
            position: fixed;
            left: 0;
            bottom: 0;
            width: 100%;
            padding: 10px;
            text-align: right;
            font-size: 14px;
            color: #666;
        }}
        .footer a {{
            color: inherit;
            margin-left: 10px;
            text-decoration: none;
        }}
        .footer a:hover {{
            text-decoration: underline;
        }}
    </style>
    <div class="footer">
        <p>
            &copy; {datetime.now().year}, made with &#128176;
            <a href="/terms" target="_self">Terms</a>
            <a href="/privacy" target="_self">Privacy</a>
        </p>
    </div>
    """
    st_module.markdown(footer_html, unsafe_allow_html=True)
