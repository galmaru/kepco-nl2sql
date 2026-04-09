"""
이천시 vs 청주시 — 업종별 전력소비 하락 분석
제조업(반도체)이 다른 업종 대비 얼마나 하락했는가
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from matplotlib import rcParams
import platform

if platform.system() == "Darwin":
    rcParams["font.family"] = "AppleGothic"
else:
    rcParams["font.family"] = "NanumGothic"
rcParams["axes.unicode_minus"] = False

conn = sqlite3.connect("data/kepco.db")

# 이천시: city = '이천시'
# 청주시: city LIKE '청주시%' (4개 구 합산)
df_raw = pd.read_sql("""
    SELECT
        CASE WHEN city LIKE '청주시%' THEN '청주시' ELSE city END AS city_grp,
        biz_type,
        year, month,
        SUM(power_usage) AS kwh
    FROM business_type
    WHERE (city = '이천시' OR city LIKE '청주시%')
      AND year BETWEEN '2022' AND '2025'
    GROUP BY city_grp, biz_type, year, month
    ORDER BY city_grp, biz_type, year, month
""", conn)
conn.close()

df_raw["ym"] = pd.to_datetime(df_raw["year"] + "-" + df_raw["month"])

# 연간 합계
df_ann = (df_raw.groupby(["city_grp","biz_type","year"])["kwh"]
          .sum().reset_index())
df_ann["kwh_b"] = df_ann["kwh"] / 1e9  # 십억kWh

# 2022 기준 정규화 (2022=100)
base = df_ann[df_ann["year"]=="2022"][["city_grp","biz_type","kwh_b"]].rename(columns={"kwh_b":"base"})
df_ann = df_ann.merge(base, on=["city_grp","biz_type"])
df_ann["idx"] = df_ann["kwh_b"] / df_ann["base"] * 100

# 업종별 2022→2025 변화율
df_rate = (df_ann[df_ann["year"].isin(["2022","2025"])]
           .pivot_table(index=["city_grp","biz_type"], columns="year", values="kwh_b")
           .reset_index())
df_rate.columns = ["city_grp","biz_type","y22","y25"]
df_rate["rate"] = (df_rate["y25"] - df_rate["y22"]) / df_rate["y22"] * 100
df_rate["delta"] = df_rate["y25"] - df_rate["y22"]

# 서비스·공공 제외
exclude = ["순수써비스","가정용부문","관  공  용","기타공공용","국  군  용","유엔군  용","농업. 임업","어      업","전      철","수      도"]
df_rate = df_rate[~df_rate["biz_type"].isin(exclude)]

# 주요 업종 월별
FOCUS = {
    "영상. 음향":  ("#2563EB", 2.8, "-",  "반도체·전자"),
    "화학  제품":  ("#DB2777", 2.0, "--", "화학제품"),
    "순수써비스":  ("#64748B", 1.5, ":",  "서비스"),
    "가정용부문":  ("#94A3B8", 1.5, ":",  "가정용"),
    "식료품제조":  ("#059669", 1.8, "--", "식료품"),
    "기타  기계":  ("#D97706", 1.8, "--", "기타기계"),
}

# ══════════════════════════════════════════════
fig = plt.figure(figsize=(18, 20))
fig.patch.set_facecolor("#F8FAFC")
plt.suptitle("이천시 vs 청주시 — 업종별 전력소비 하락 분석 (2022~2025)",
             fontsize=16, fontweight="bold", y=1.01, color="#1E293B")

def style(ax, title, grid_axis="y"):
    ax.set_facecolor("#FFFFFF")
    ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
    ax.grid(axis=grid_axis, ls="--", alpha=0.35, color="#CBD5E1")
    ax.spines[["top","right"]].set_visible(False)
    ax.spines[["left","bottom"]].set_color("#94A3B8")

# ──────────────────────────────────────────────
# [상단 2개] 도시별 업종 증감률 수평 막대
# ──────────────────────────────────────────────
for col_idx, city in enumerate(["이천시","청주시"]):
    ax = fig.add_subplot(4, 2, col_idx+1)
    sub = df_rate[df_rate["city_grp"]==city].dropna().sort_values("rate")

    bar_colors = []
    for _, row in sub.iterrows():
        if row["biz_type"] == "영상. 음향":
            bar_colors.append("#2563EB")
        elif row["biz_type"] == "화학  제품":
            bar_colors.append("#DB2777")
        elif row["rate"] < 0:
            bar_colors.append("#FCA5A5")
        else:
            bar_colors.append("#86EFAC")

    bars = ax.barh(sub["biz_type"].str.strip(), sub["rate"],
                   color=bar_colors, alpha=0.88, height=0.65, edgecolor="white")
    ax.axvline(0, color="#374151", lw=1.2)

    for bar, val in zip(bars, sub["rate"]):
        if abs(val) > 3:
            x = val - 1 if val < 0 else val + 1
            ax.text(x, bar.get_y()+bar.get_height()/2, f"{val:+.0f}%",
                    va="center", ha="right" if val<0 else "left", fontsize=8)

    # 반도체 주석
    if "영상. 음향" in sub["biz_type"].values:
        semi_rate = sub[sub["biz_type"]=="영상. 음향"]["rate"].values[0]
        ax.axvline(semi_rate, color="#2563EB", lw=1.5, ls=":", alpha=0.7)
        ax.text(semi_rate, len(sub)*0.95, f"반도체\n{semi_rate:+.0f}%",
                color="#2563EB", fontsize=8.5, fontweight="bold", ha="center")

    ax.set_xlabel("전력사용량 증감률 (%)", fontsize=9)
    total_rate = (sub["y25"].sum() - sub["y22"].sum()) / sub["y22"].sum() * 100
    style(ax, f"[{col_idx+1}] {city} 업종별 증감률 (2022→2025)\n전체 평균 {total_rate:+.1f}%", "y")

# ──────────────────────────────────────────────
# [중단 2개] 절대량 기준 업종별 기여 (상위만)
# ──────────────────────────────────────────────
for col_idx, city in enumerate(["이천시","청주시"]):
    ax = fig.add_subplot(4, 2, col_idx+3)
    sub = df_rate[df_rate["city_grp"]==city].dropna()
    sub = sub[sub["y22"] > 0.01].sort_values("delta")

    bar_colors = ["#2563EB" if b=="영상. 음향" else ("#FCA5A5" if v<0 else "#86EFAC")
                  for b, v in zip(sub["biz_type"], sub["delta"])]

    ax.barh(sub["biz_type"].str.strip(), sub["delta"],
            color=bar_colors, alpha=0.88, height=0.65, edgecolor="white")
    ax.axvline(0, color="#374151", lw=1.2)

    for bar, val in zip(ax.patches, sub["delta"]):
        if abs(val) > 0.01:
            x = val - 0.005 if val < 0 else val + 0.005
            ax.text(x, bar.get_y()+bar.get_height()/2, f"{val:+.2f}",
                    va="center", ha="right" if val<0 else "left", fontsize=8)

    ax.set_xlabel("전력사용량 변화 (십억 kWh)", fontsize=9)
    style(ax, f"[{col_idx+3}] {city} 업종별 절대 감소량\n(2022→2025, 십억 kWh)", "x")
    ax.grid(axis="x", ls="--", alpha=0.35, color="#CBD5E1")
    ax.grid(axis="y", visible=False)

# ──────────────────────────────────────────────
# [하단 좌] 2022=100 기준 지수 비교 (이천시)
# ──────────────────────────────────────────────
ax5 = fig.add_subplot(4, 2, 5)
city = "이천시"
top_biz = (df_rate[df_rate["city_grp"]==city]
           .dropna().sort_values("y22", ascending=False)
           .head(6)["biz_type"].tolist())

palette = ["#2563EB","#DB2777","#059669","#D97706","#7C3AED","#64748B"]
for i, biz in enumerate(top_biz):
    sub = df_ann[(df_ann["city_grp"]==city) & (df_ann["biz_type"]==biz)].sort_values("year")
    lw = 2.8 if biz == "영상. 음향" else 1.6
    ax5.plot(sub["year"].astype(int), sub["idx"], lw=lw, marker="o", ms=6,
             label=biz.strip(), color=palette[i])

ax5.axhline(100, color="#94A3B8", lw=1, ls="--")
ax5.set_ylabel("전력사용량 지수 (2022=100)", fontsize=9)
ax5.set_xticks([2022,2023,2024,2025])
ax5.legend(fontsize=8.5, loc="lower left", ncol=2)
ax5.annotate("반도체 감산\n(-43%)", xy=(2025, df_ann[(df_ann["city_grp"]==city)&(df_ann["biz_type"]=="영상. 음향")&(df_ann["year"]=="2025")]["idx"].values[0]),
             xytext=(2023.3, 45), fontsize=8.5, color="#2563EB", fontweight="bold",
             arrowprops=dict(arrowstyle="->", color="#2563EB"))
style(ax5, "[5] 이천시 — 2022=100 기준 업종별 지수 추이")

# ──────────────────────────────────────────────
# [하단 우] 2022=100 기준 지수 비교 (청주시)
# ──────────────────────────────────────────────
ax6 = fig.add_subplot(4, 2, 6)
city = "청주시"
top_biz_cj = (df_rate[df_rate["city_grp"]==city]
              .dropna().sort_values("y22", ascending=False)
              .head(6)["biz_type"].tolist())

for i, biz in enumerate(top_biz_cj):
    sub = df_ann[(df_ann["city_grp"]==city) & (df_ann["biz_type"]==biz)].sort_values("year")
    lw = 2.8 if biz == "영상. 음향" else 1.6
    ax6.plot(sub["year"].astype(int), sub["idx"], lw=lw, marker="o", ms=6,
             label=biz.strip(), color=palette[i % len(palette)])

ax6.axhline(100, color="#94A3B8", lw=1, ls="--")
ax6.set_ylabel("전력사용량 지수 (2022=100)", fontsize=9)
ax6.set_xticks([2022,2023,2024,2025])
ax6.legend(fontsize=8.5, loc="lower left", ncol=2)
style(ax6, "[6] 청주시 — 2022=100 기준 업종별 지수 추이")

# ──────────────────────────────────────────────
# [하단 전체] 이천·청주 반도체 vs 나머지 월별
# ──────────────────────────────────────────────
ax7 = fig.add_subplot(4, 1, 4)

for city, c_semi, c_rest, ls_rest in [("이천시","#2563EB","#93C5FD","--"),
                                        ("청주시","#DC2626","#FCA5A5","--")]:
    # 반도체(영상.음향)
    semi = (df_raw[(df_raw["city_grp"]==city) & (df_raw["biz_type"]=="영상. 음향")]
            .groupby("ym")["kwh"].sum() / 1e9)
    # 나머지 전체
    rest = (df_raw[(df_raw["city_grp"]==city) & (df_raw["biz_type"]!="영상. 음향")]
            .groupby("ym")["kwh"].sum() / 1e9)

    ax7.plot(semi.index, semi, lw=2.5, color=c_semi, label=f"{city} 반도체·전자")
    ax7.plot(rest.index, rest, lw=1.8, ls=ls_rest, color=c_rest, label=f"{city} 나머지 업종 합산")

for year in ["2023","2024","2025"]:
    ax7.axvline(pd.Timestamp(f"{year}-01-01"), color="#CBD5E1", lw=1, ls=":")
    ax7.text(pd.Timestamp(f"{year}-02-01"), ax7.get_ylim()[1]*0.97 if ax7.get_ylim()[1] else 5,
             year, fontsize=9, color="#64748B")

ax7.set_ylabel("전력사용량 (십억 kWh)", fontsize=10)
ax7.legend(fontsize=9.5, ncol=2, loc="lower left")
style(ax7, "[7] 이천·청주 — 반도체 vs 나머지 업종 월별 전력사용량")

plt.tight_layout(h_pad=3.5, w_pad=3)
import os; os.makedirs("analysis/output", exist_ok=True)
out = "analysis/output/city_industry_breakdown.png"
plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"저장: {out}")
