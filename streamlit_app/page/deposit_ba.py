"""Revenue page for first deposits from data_depo_ba."""

from streamlit_app.page.deposit import render_deposit_page


async def show_deposit_ba_page(host: str) -> None:
    await render_deposit_page(
        host, title="First Deposit BA", uri="deposit/ba-report", state_prefix="deposit_ba",
    )
