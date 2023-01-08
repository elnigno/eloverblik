import streamlit as st
import eloverblik.dashboard
from eloverblik.eloverblik import DatabaseBuilder

st.header("Electricity overview")

if st.button("Update database"):
    db = DatabaseBuilder()
    db.update_dataset()
    st.write("Database updated")
else:
    st.write(" ")

st.markdown(
    """
    The dashboard uses data from [Eloverblik](https://eloverblik.dk/welcome).
    The earliest available data is from January 2019.
    """
)

st.write(
    """
    ### Consumption overview

    **Total electricity consumption by meter ID and year (kWh)**
    """
)
st.write(eloverblik.dashboard.get_overall_consumption())

meterid = st.selectbox(
    "Select a meter ID for more detailed information",
    eloverblik.dashboard.get_meterids_in_db(),
)

st.write(
    """
    The graph below shows how your yearly electricity consumption has changed
    over time, for each measurement point.
    """
)

st.write(eloverblik.dashboard.chart_year_rolling_avgs(meterid=meterid))


st.write(
    """
**Hourly electricity consumption**

No matter if you pay variable or fixed electricity prices, the tariffs for the
transport of electricity will depend on the time of consumption. Below you find
an overview of your hourly tariffs and its composition.
"""
)

st.write(eloverblik.dashboard.current_tariffs_graph(meterid))

st.write(
    """
It can then be useful to understand your hourly consumption patterns. The graph
below allows you to see what your average hourly consumption  has been in recent
years and seasons. You can compare two periods using the selection menus.
"""
)

col1, col2 = st.columns(2)
with col1:
    st.write(
        """<span style="color:red"><b>Period 1<b></span>""", unsafe_allow_html=True
    )
    years1 = st.multiselect(
        "Years",
        eloverblik.dashboard.get_years_in_db(),
        eloverblik.dashboard.get_years_in_db(),
        key="years1",
    )
    summer1 = st.checkbox("Summer", key="summer1", value=True)
    winter1 = st.checkbox("Winter", key="winter1")

with col2:
    st.write(
        """<span style="color:blue"><b>Period 2<b></span>""", unsafe_allow_html=True
    )
    years2 = st.multiselect(
        "Years",
        eloverblik.dashboard.get_years_in_db(),
        eloverblik.dashboard.get_years_in_db(),
        key="years2",
    )
    summer2 = st.checkbox("Summer", key="summer2")
    winter2 = st.checkbox("Winter", key="winter2", value=True)

st.write(
    eloverblik.dashboard.hourly_consumption_wrapper(
        meterid, years1, summer1, winter1, years2, summer2, winter2, width=0.35
    )
)
