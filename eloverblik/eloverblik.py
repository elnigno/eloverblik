import eloverblik.tools as tools
import requests
import duckdb
import pandas as pd
from datetime import datetime, date, timedelta


class Downloader:
    """
    Accesses eloverblik's API, storing meterid and data access token for
    multiple calls. Currently only downloads consumption data. See
    https://api.eloverblik.dk/customerapi/index.html
    """

    def __init__(self) -> None:
        self.meterids = None
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

    def get_meter_info(self) -> str:
        """ """
        if self.data_access_token is None:
            self.update_data_access_token()

        metering_points_url = "https://api.eloverblik.dk/CustomerApi/api/meteringpoints/meteringpoints"
        headers = tools.get_headers(self.data_access_token)
        meters = requests.get(metering_points_url, headers=headers)
        df = pd.json_normalize(meters.json()["result"])

        if self.meterids is None:
            self.meterids = df['meteringPointId'].tolist()

        return df

    def get_meter_ids(self):
        if self.meterids is None:
            self.get_meter_info()

        return self.meterids

    def get_charges(self, meterid=None) -> str:
        """ """
        if self.data_access_token is None:
            self.update_data_access_token()

        if meterid is None:
            meterid = self.get_meter_ids()[0]

        meter_charges_url = "https://api.eloverblik.dk/CustomerApi/api/meteringpoints/meteringpoint/getcharges"
        headers = tools.get_headers(self.data_access_token)

        meter_json = {"meteringPoints": {"meteringPoint": [meterid]}}

        data = requests.post(
            meter_charges_url, headers=headers, json=meter_json
        )

        return data

    def get_consumption(self, fromdate, todate, agg="Hour", meterid=None):
        """
        If `meterid` is None it will take the first meterid in the list
        """

        if self.data_access_token is None:
            self.update_data_access_token()
        if meterid is None:
            meterid = self.get_meter_ids()[0]

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
    def __init__(self) -> None:
        super().__init__()
        self.data_dir = tools.datapath

    def get_min_date(meterid):
        with duckdb.connect(str(tools.datapath), read_only=True) as conn:
            v = conn.execute(
                f"""
                select greatest(consumerStartDate, DATE '2019-01-01') as startdate
                from meterinfo where meteringPointId={meterid}
                """
            ).fetchone()[0]
        return v

    def build_consumption_table(self):

        conn = duckdb.connect(str(self.data_dir), read_only=False)
        conn.execute("DROP TABLE IF EXISTS consumption;")
        conn.execute(tools.tabledef_consumption("consumption"))

        # Get consumption data
        for meterid in self.get_meter_ids():
            mindate = self.get_min_date(meterid)
            minyear = int(mindate[:4])
            datalist = []

            for year in range(minyear, datetime.today().year):
                # Determine when to start querying the API
                if year == minyear:
                    startdate = mindate
                else:
                    startdate = f"{year}-01-01"

                datalist += [
                    tools.data_to_df(
                        self.get_consumption(startdate, f"{year+1}-01-01", meterid=meterid)
                    )
                ]
            if minyear < datetime.today().year:
                startdate = f"{datetime.today().year}-01-01"
            else:
                startdate = mindate
            datalist += [
                tools.data_to_df(
                    self.get_consumption(
                        startdate,
                        f"{str(date.today())}",
                        meterid=meterid
                    )
                )
            ]
            data = pd.concat(datalist).reset_index(drop=True)
            data['meterid'] = meterid

            conn.execute("INSERT INTO consumption SELECT meterid, date, kWh FROM data")

        conn.close()

        return

    def build_tariffs_dataset(self) -> None:
        """
        """
        tdata = []
        for meterid in self.get_meter_ids():
            data = self.get_charges()
            tariffs = tools.extract_tariffs(data)
            tariffs['meterid'] = meterid
            tdata += [tariffs]

        tdata = pd.concat(tdata)

        conn = duckdb.connect(str(self.data_dir), read_only=False)
        conn.execute("DROP TABLE IF EXISTS current_tariffs;")
        conn.execute("CREATE TABLE current_tariffs AS SELECT * FROM tdata")
        conn.close()

        return

    def build_dataset(self) -> None:
        meter_info = self.get_meter_info()
        conn = duckdb.connect(str(self.data_dir), read_only=False)
        conn.execute("DROP TABLE IF EXISTS meterinfo;")
        meter_info.shape
        conn.execute("CREATE TABLE meterinfo AS SELECT * FROM meter_info")
        conn.close()

        self.build_consumption_table()

        self.build_tariffs_dataset()

        return

    def update_dataset(self) -> pd.DataFrame:
        conn = duckdb.connect(str(self.data_dir), read_only=False)
        for meterid in self.get_meter_ids():
            lastdate = (
                conn.execute(f"select MAX(date) from consumption where meterid={meterid}")
                .fetchall()[0][0]
                .date()
            )
            if date.today() - timedelta(days=1) > lastdate:
                updatedf = tools.data_to_df(
                    self.get_consumption(
                        lastdate + timedelta(days=1), f"{str(date.today())}", meterid=meterid
                    )
                )
                updatedf['meterid'] = meterid
                conn.execute("insert into consumption select meterid, date, kWh from updatedf")
            else:
                print("Database is already updated")
        conn.close()
        return
