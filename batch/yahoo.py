import sys
import yfinance as yf
import pandas as pd
import sqlite3
import jpx
import en2ja

to_ja = en2ja.to_ja


def update_price_data(stocks: pd.DataFrame) -> None:
    """
    Yahoo Finance から株価データを取得して SQLite に保存します。

    Args:
        stocks (pd.DataFrame): 銘柄コードの DataFrame。'コード' 列を含む必要があります。
    """

    print("Yahoo Finance から株価データを取得中...")

    conn = sqlite3.connect("db.sqlite3")

    # 各銘柄コードについて株価データを取得
    for code in ["N225"] + stocks["コード"].tolist():

        print(f"コードを処理中: {code}")

        # Yahoo Finance のティッカーオブジェクトを取得
        if code == "N225":
            ticker = yf.Ticker("^N225")
        else:
            ticker = yf.Ticker(f"{code}.T")

        # 株価テーブルが存在するか確認
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='株価データ';")
        table_exists = cursor.fetchone() is not None
        if table_exists:
            existing_dates = pd.read_sql(f"SELECT max(日付) as 日付 FROM 株価データ WHERE コード = '{code}'", conn)["日付"].tolist()
        else:
            existing_dates = []

        if len(existing_dates) > 0 and existing_dates[0] is not None:
            latest_date = max(existing_dates)
            print(f"既存データの最新日付: {latest_date}")
            # 既存データの最新日付以降のデータを取得
            df = ticker.history(start=latest_date)
            # 最新日付のデータは重複する可能性があるため削除
            df = df[df.index > latest_date]
        else:
            # 既存データがない場合は5年分のデータを取得
            print("株価データが存在しません。5年分のデータを取得します。")
            df = ticker.history(period="5y")

        # インデックスを日付列に変換
        df = df.reset_index()

        # Date 列を日付型に変換
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        # Date, Open, High, Low, Close, Volume 列のみ抽出
        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]

        # Date, Open, High, Low, Close, Volume を日本語に変換
        df = df.rename(columns={"Date": "日付", "Open": "始値", "High": "高値", "Low": "安値", "Close": "終値", "Volume": "出来高"})

        # 銘柄コードの列を追加
        df["コード"] = code

        # インデックスをコード、日付に設定
        df = df.set_index(["コード", "日付"]).reset_index()

        # 株価データテーブルに追加
        df.to_sql("株価データ", conn, if_exists="append", index=False)


def update_financial_data(stocks: pd.DataFrame, from_local: bool = False) -> None:
    """
    Yahoo Finance から各種財務データを取得して SQLite に保存します。

    Args:
        stocks (pd.DataFrame): 銘柄コードの DataFrame。'コード' 列を含む必要があります。
        from_local (bool): ローカルデータから読み込む場合は True。デフォルトは False でYahooから取得します。
    """

    # 各種データの初期化
    all_data = [
        [None, "financials", "財務諸表_FIN", "fin"],
        [None, "balance_sheet", "賃借対照表_BS", "bs"],
        [None, "financials", "損益計算書_PL", "pl"],
        [None, "cashflow", "キャッシュフロー計算書_CF", "cf"],
        [None, "quarterly_financials", "財務諸表_FINQTY", "fin"],
        [None, "quarterly_balance_sheet", "賃借対照表_BSQTY", "bs"],
        [None, "quarterly_income_stmt", "損益計算書_PLQTY", "pl"],
        [None, "quarterly_cashflow", "キャッシュフロー計算書_CFQTY", "cf"],
    ]

    if from_local:
        # ローカルデータから読み込み
        print("ローカルデータから各種データを読み込み中...")

        for data in all_data:
            conn = sqlite3.connect("db.sqlite3")
            data[0] = pd.read_sql(f"SELECT * FROM {data[2]}", conn)

    else:
        # Yahoo Finance からデータを取得
        print("Yahoo Finance から各種データを取得中...")

        for code in stocks["コード"].values:

            print(f"コードを処理中: {code}")

            # Yahoo Finance のティッカーオブジェクトを取得
            ticker = yf.Ticker(f"{code}.T")

            # 各種データを取得して追加
            for data in all_data:
                data[0] = append_table(data[0], getattr(ticker, data[1]), code)

            # 貸借対照表ログテーブルを読み込んで横持ち変換する
            data[0] = data[0].pivot_table(index=["コード", "決算期"], columns=["項目"], values="値").reset_index()

    # データを SQLite に保存
    for data in all_data:
        store_data(data[0], data[2], data[3])


def append_table(df_all: pd.DataFrame, df: pd.DataFrame, code: str) -> None:
    """
    DataFrame のデータを 縦持ちに変換して df_all に追加します。

    Args:
        df_all (pd.DataFrame): 追加先の DataFrame。最初は None で渡します。
        df (pd.DataFrame): 追加する DataFrame。
        code (str): 銘柄コード。
    """
    # データを縦持ちに変換
    df = df.reset_index().melt(id_vars=["index"])
    df.columns = ["項目", "決算期", "値"]

    # 決算期を日付型に変換
    df["決算期"] = pd.to_datetime(df["決算期"]).dt.date

    # 銘柄コードの列を追加
    df["コード"] = code

    # インデックスをコード、項目、決算期に設定
    df = df.set_index(["コード", "決算期", "項目"]).reset_index()

    # df_all に追加
    if df_all is None:
        df_all = df
    else:
        df_all = pd.concat([df_all, df], ignore_index=True)

    return df_all


def store_data(df: pd.DataFrame, table_name: str, data_type: str) -> None:
    """
    DataFrame を SQLite に保存します。

    Args:
        df (pd.DataFrame): 保存する DataFrame。
        table_name (str): 保存先のテーブル名。
        data_type (str): データ種別。列名変換に使用します。
    """

    # 日本語変換辞書を取得
    my_to_ja = to_ja[data_type]

    # 辞書にない列名をログに出力
    for col in df.columns:
        if col not in my_to_ja and col not in ["コード", "決算期"]:
            if col not in my_to_ja.values():
                print(f"未登録の列名: {col} （データ種別: {data_type}）")

    # 列名を日本語に変換
    df = df.rename(columns=my_to_ja) if data_type in to_ja else df

    # テーブルをドロップクリエイト
    conn = sqlite3.connect("db.sqlite3")
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()

    df.to_sql(table_name, conn, if_exists="replace", index=False)


def rename_columns(table_name: str, data_type: str) -> None:
    """
    SQLLiteのテーブルの列名を日本語に変換します。
    """
    my_to_ja = to_ja[data_type]

    # テーブルを読み込み
    conn = sqlite3.connect("db.sqlite3")
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # 辞書にない列名をログに出力
    for col in df.columns:
        if col not in my_to_ja and col not in ["コード", "決算期"]:
            print(f"未登録の列名: {col} （データ種別: {data_type}）")

    # 列名を日本語に変換
    df = df.rename(columns=my_to_ja) if data_type in to_ja else df

    return df


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "fin":
        print("JPX銘柄一覧を読み込み中...")
        stocks = jpx.load()

        # localオプションの確認
        if len(sys.argv) > 2 and sys.argv[2] == "local":
            from_local = True
        else:
            from_local = False

        # 市場・商品区分がプライムのみに絞り込み
        stocks = stocks[stocks["市場・商品区分"] == "プライム"]

        print("各種データを更新中...")
        update_financial_data(stocks, from_local=from_local)
        # create_data()
        print("各種データの更新が完了しました。")
    elif len(sys.argv) > 1 and sys.argv[1] == "price":
        print("JPX銘柄一覧を読み込み中...")
        stocks = jpx.load()

        # 市場・商品区分がプライムのみに絞り込み
        stocks = stocks[stocks["市場・商品区分"] == "プライム"]

        print("株価データを更新中...")
        update_price_data(stocks)
        print("株価データの更新が完了しました。")
    else:
        print("usage: python yahoo.py fin [local] or price")
