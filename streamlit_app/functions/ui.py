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
    </style>
    <div class="footer">
        <p>© {datetime.now().year}, made with 💰</p>
    </div>
    """
    st_module.markdown(footer_html, unsafe_allow_html=True)
