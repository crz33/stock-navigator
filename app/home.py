import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px

# ページ設定
st.set_page_config(page_title="株式ナビ", layout="wide")

# 期間の選択肢リスト
PERIOD_OPTIONS = [("1年", 12), ("6ヶ月", 6), ("3ヶ月", 3), ("1ヶ月", 1), ("2週間", 0.5), ("1週間", 0.25)]

with st.sidebar:
    # ページ選択
    page = st.radio("ページを選択", ["N225", "17業種別指数", "設定"])

    # 期間選択で1ヶ月,3ヶ月,6ヶ月,1年を選択できるようにする
    period = st.radio("期間", [item[0] for item in PERIOD_OPTIONS], index=0)

# SQLiteからデータを取得する
DB_PATH = "data/stocks.db"


# 指定された期間のデータを読み込む関数
@st.cache_data(show_spinner=False)
def load(code_list: list, period: float) -> pd.DataFrame:
    try:
        with sqlite3.connect("db.sqlite3") as conn:

            # code_listに基づいてSQLクエリを動的に生成
            codes_str = ",".join([f"'{code}'" for code in code_list])
            query = f"select * from 株価データ where コード in ({codes_str})"
            df = pd.read_sql_query(query, conn)

            # 日付のyyyMMdd形式のintegerをdate型に変換して読み込む
            df["日付"] = pd.to_datetime(df["日付"], format="%Y-%m-%d")

            # 期間に応じてデータをフィルタリング
            if period < 1:
                df = df[df["日付"] >= pd.Timestamp.now() - pd.DateOffset(weeks=int(period * 4))]
            else:
                df = df[df["日付"] >= pd.Timestamp.now() - pd.DateOffset(months=int(period))]
            # 日付、コードが"0000","0002"、⋯で横持ちに変換する
            df = df.pivot(index="日付", columns="コード", values="終値").reset_index()
            df.columns.name = None
            return df
    except Exception as e:
        st.error(f"DB読み込みエラー: {e}")
        return None


if page == "N225":
    st.subheader("🏠 日経225")

    # データ読み込み
    df = load(["N225"], next(item for item in PERIOD_OPTIONS if item[0] == period)[1])

    # 日経225のグラフを表示
    if df is not None:
        fig = px.line(df, x="日付", y="N225", labels={"日付": "日付", "N225": "日経225"})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("データが取得できませんでした。")


elif page == "17業種別指数":
    st.subheader("📈 17業種別指数チャート")

    # データ読み込み
    df = load([], next(item for item in PERIOD_OPTIONS if item[0] == period)[1])

    if df is None:
        st.warning("データが取得できませんでした。")
    else:
        # dfのカラム名からTOPXIX-17で始まる列のみ抽出
        industry_indices = [col for col in df.columns if col.startswith("TOPIX-17")]

        # 一番古い日付を基準日としてインデックス化する
        base_date = df["日付"].min()
        for index in industry_indices:
            if index in df.columns:
                base_value = df.loc[df["日付"] == base_date, index].values[0]
                df[index] = df[index] / base_value

        # 業種別指数のグラフを表示
        fig = px.line(
            df,
            x="日付",
            y=industry_indices,
            labels={"日付": "日付", "value": "指数", "variable": "業種"},
        )
        st.plotly_chart(fig, use_container_width=True)

elif page == "設定":
    st.subheader("設定")
    dark = st.checkbox("ダークモード (デモ)")
    st.write(f"ダークモード: {'ON' if dark else 'OFF'}")
