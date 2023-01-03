import eloverblik.tools as tools
import requests
import duckdb
import pandas as pd
from datetime import datetime, date, timedelta


class Downloader:
    """ """

    def __init__(self, meterid=None) -> None:
        self.meterid = meterid
        self.data_access_token = None
        return

    @staticmethod
    def read_refresh_token() -> str:
        with open("token.txt") as f:
            refresh_token = f.readline()
        return refresh_token

    def update_data_access_token(self) -> str:
        """ """
        get_data_access_token_url = "https://api.eloverblik.dk/CustomerApi/api/token"
        headers = tools.get_headers(self.read_refresh_token())

        response = requests.get(get_data_access_token_url, headers=headers)
        data_access_token = response.json()["result"]

        self.data_access_token = data_access_token

        return

    def get_meter_id(self) -> str:
        """ """
        if self.data_access_token is None:
            self.update_data_access_token()
        if self.meterid is None:
            metering_points_url = "https://api.eloverblik.dk/CustomerApi/api/meteringpoints/meteringpoints"
            headers = tools.get_headers(self.data_access_token)
            meters = requests.get(metering_points_url, headers=headers)
            first_meter = meters.json()["result"][0]["meteringPointId"]

            self.meterid = first_meter

            return first_meter
        else:
            return self.meterid

    def get_consumption(self, fromdate, todate, agg="Hour", meterid=None):
        """ """

        if self.data_access_token is None:
            self.update_data_access_token()
        if meterid is None:
            meterid = self.get_meter_id()

        meter_data = (
            "https://api.eloverblik.dk/CustomerApi/api/meterdata/gettimeseries/"
        )
        headers = tools.get_headers(self.data_access_token)
        meter_data_url = f"{meter_data}{fromdate}/{todate}/{agg}"

        meter_json = {"meteringPoints": {"meteringPoint": [meterid]}}

        meter_data_request = requests.post(
            meter_data_url, headers=headers, json=meter_json
        )

        return meter_data_request


class DatabaseBuilder(Downloader):
    def __init__(self, meterid=None) -> None:
        super().__init__(meterid=meterid)
        self.data_dir = tools.datapath

    def build_dataset(self) -> pd.DataFrame:
        datalist = []
        for year in range(2019, datetime.today().year):
            datalist += [
                tools.data_to_df(
                    self.get_consumption(f"{year}-01-01", f"{year+1}-01-01")
                )
            ]
        datalist += [
            tools.data_to_df(
                self.get_consumption(
                    f"{datetime.today().year}-01-01",
                    f"{str(date.today() - timedelta(days=1))}",
                )
            )
        ]
        data = pd.concat(datalist).reset_index(drop=True)

        conn = duckdb.connect(str(self.data_dir), read_only=False)
        conn.execute("DROP TABLE IF EXISTS consumption;")
        conn.execute(tools.tabledef_consumption("consumption"))
        conn.execute("INSERT INTO consumption SELECT * FROM data")
        conn.close()
        return data

    def update_dataset(self) -> pd.DataFrame:
        conn = duckdb.connect(str(self.data_dir), read_only=False)
        lastdate = (
            conn.execute("select MAX(date) from consumption")
            .fetchall()[0][0]
            .date()
        )
        if date.today() - timedelta(days=1) > lastdate:
            updatedf = self.get_consumption(
                lastdate, f"{str(date.today() - timedelta(days=1))}"
            )
            conn.execute("insert into consumption select * from updatedf")
            updatedf.shape
        else:
            print("Database is already updated")
        conn.close()
        return
