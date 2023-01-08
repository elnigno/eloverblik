import duckdb
import altair as alt
import eloverblik.tools
import matplotlib.pyplot as plt


def get_meterids_in_db():
    """ """
    conn = duckdb.connect(str(eloverblik.tools.datapath), read_only=True)
    meterids = [
        i[0]
        for i in conn.execute("""select meteringPointId from meterinfo""").fetchall()
    ]
    return meterids


def get_years_in_db():
    """ """
    conn = duckdb.connect(str(eloverblik.tools.datapath), read_only=True)
    meterids = [
        i[0]
        for i in conn.execute(
            """select YEAR(date) as year from consumption group by YEAR(date)"""
        ).fetchall()
    ]
    return meterids


def get_overall_consumption():
    conn = duckdb.connect(str(eloverblik.tools.datapath), read_only=True)
    df = (
        conn.execute(
            """
        select c.meterid as 'Meter ID', YEAR(c.date) as 'Year'
        , CAST(SUM(c.kWh) as INT) as 'Total kWh consumption'
        , FIRST(CONCAT_WS(' ', mi.streetName, mi.buildingNumber, mi.floorId || mi.roomId || ',', cityName)) as address
        from consumption as c
        inner join meterinfo as mi
        on c.meterid=mi.meteringPointId
        group by YEAR(date), meterid
        """
        )
        .df()
        .pivot(
            index=["Meter ID", "address"],
            columns="Year",
            values=["Total kWh consumption"],
        )
    )
    varnames = ["Meter ID", "Address"] + list(df.columns.get_level_values(1))
    df = df.reset_index()
    df.columns = varnames
    df = df.set_index("Meter ID")
    conn.close()
    return df


def chart_year_rolling_avgs(meterid=None):
    """ """
    conn = duckdb.connect(str(eloverblik.tools.datapath), read_only=True)
    if meterid is None:
        meterid = conn.execute("select MIN(meterid) from consumption").fetchone()[0]
    # Get data
    df = conn.execute(
        f"""
        SELECT
            round(AVG(kWh_day) OVER(ROWS between 7 preceding and 7 following), 2) as rolling_average
            , cast(YEAR(date) as string) as year
            , date - interval (year(date) - 2019) YEAR as newdate
            , strftime(date, '%-d %b') as strdate
        from (
            select date_trunc('day', date) as date,
            SUM(kWh) as kWh_day
            from consumption
            where meterid = {meterid}
            group by date_trunc('day', date)
            ) as c
        """
    ).df()
    # Close connection
    conn.close()

    nearest = alt.selection(
        type="single", nearest=True, on="mouseover", fields=["newdate"], empty="none"
    )
    lines = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("newdate", axis=alt.Axis(format="%b", title="")),
            y=alt.X("rolling_average", axis=alt.Axis(title="")),
            color="year",
        )
    ).properties(title="Daily kWh consumption, 7-days rolling average")

    selectors = (
        alt.Chart(df)
        .mark_point()
        .encode(x="newdate", opacity=alt.value(0), tooltip="strdate")
        .add_selection(nearest)
    )

    # Draw points on the line, and highlight based on selection
    points = lines.mark_point().encode(
        opacity=alt.condition(nearest, alt.value(1), alt.value(0))
    )

    # Draw text labels near the points, and highlight based on selection
    text = lines.mark_text(align="left", dx=5, dy=-5).encode(
        text=alt.condition(nearest, "rolling_average", alt.value(" "))
    )

    # Put the five layers into a chart and bind the data
    chart = alt.layer(lines, selectors, points, text).properties(width=700, height=400)
    return chart


def get_avg_hourly_consumption(conn, meterid, years, summer=True, winter=True):
    seasons = []
    if summer:
        seasons += ["'summer'"]
    if winter:
        seasons += ["'winter'"]
    if len(seasons) == 0:
        seasons = "('None')"
    else:
        seasons = f"""({",".join(seasons)})"""

    df = conn.execute(
        f"""
        select HOUR(date) as hour
        , AVG(kWh) as tot
        from (
            select *
            , CASE when month(date) between 4 and 9 then 'summer' else 'winter' end as season
            from consumption
            where year(date) in ({','.join([str(y) for y in years])})
            and month(date)>3
        ) as c
        WHERE season in {seasons}
        group by HOUR(date)
        """
    ).df()
    return df


def chart_hourly_consumption_bysample(df1, df2, width=0.35):
    fig, ax = plt.subplots(1, 1, figsize=(10, 5), sharey=True)

    width = 0.35

    ax.bar(df1["hour"] - width / 2, df1["tot"], width, label="Period 1", color="red")
    ax.bar(df2["hour"] + width / 2, df2["tot"], width, label="Period 2", color="blue")

    ax.set_xlabel("Time of day")
    # ax.set_ylabel('kWh', rotation=0, y=0.95, ha='left', labelpad=5)
    ax.legend(frameon=False)

    ax.set_xlim(-0.5, 23.5)
    ax.set_xticks(range(0, 24))

    ax.axvspan(-0.5, 5.5, color="green", alpha=0.2)
    ax.axvspan(5.5, 16.5, color="yellow", alpha=0.2)
    ax.axvspan(16.5, 20.5, color="red", alpha=0.2)
    ax.axvspan(20.5, 24.5, color="yellow", alpha=0.2)
    ax.set_title("Average hourly kWh consumption in selected period", ha="right")
    return fig


def hourly_consumption_wrapper(
    meterid, years1, summer1, winter1, years2, summer2, winter2, width=0.35
):
    conn = duckdb.connect(str(eloverblik.tools.datapath), read_only=True)
    df1 = get_avg_hourly_consumption(
        conn, meterid, years1, summer=summer1, winter=winter1
    )
    df2 = get_avg_hourly_consumption(
        conn, meterid, years2, summer=summer2, winter=winter2
    )
    return chart_hourly_consumption_bysample(df1, df2, width=width)
