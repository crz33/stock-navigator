import sys
import yfinance as yf
import pandas as pd
import sqlite3
import jpx
import en2ja

to_ja = en2ja.to_ja


def update_financial_data(stocks: pd.DataFrame, from_local: bool = False) -> None:
    """
    Yahoo Finance から各種財務データを取得して SQLite に保存します。

    Args:
        stocks (pd.DataFrame): 銘柄コードの DataFrame。'コード' 列を含む必要があります。
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
    DataFrame のデータを SQLite のテーブルに追加します。
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
    if len(sys.argv) > 1 and sys.argv[1] == "update":
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
    else:
        print("usage: python yahoo.py update")
