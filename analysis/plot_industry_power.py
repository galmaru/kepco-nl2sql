"""
업종별 전력소비 분석 — 2025년 하락 원인 규명
반도체(영상.음향) vs 기타 제조업 비교
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib import rcParams
import platform

if platform.system() == "Darwin":
    rcParams["font.family"] = "AppleGothic"
else:
    rcParams["font.family"] = "NanumGothic"
rcParams["axes.unicode_minus"] = False

conn = sqlite3.connect("data/kepco.db")

# ── 데이터 수집
df_annual = pd.read_sql("""
    SELECT biz_type,
        SUM(CASE WHEN year='2022' THEN power_usage ELSE 0 END) AS y22,
        SUM(CASE WHEN year='2023' THEN power_usage ELSE 0 END) AS y23,
        SUM(CASE WHEN year='2024' THEN power_usage ELSE 0 END) AS y24,
        SUM(CASE WHEN year='2025' THEN power_usage ELSE 0 END) AS y25
    FROM business_type
    WHERE year BETWEEN '2022' AND '2025'
    GROUP BY biz_type
    HAVING y22 > 0 AND y25 > 0
    ORDER BY y22 DESC
""", conn)
df_annual["rate_22_25"] = (df_annual["y25"] - df_annual["y22"]) / df_annual["y22"] * 100
df_annual["abs_change"] = (df_annual["y25"] - df_annual["y22"]) / 1e9  # 십억kWh

# 월별 주요 업종
df_monthly = pd.read_sql("""
    SELECT year, month, biz_type, SUM(power_usage) AS kwh
    FROM business_type
    WHERE year BETWEEN '2022' AND '2025'
    GROUP BY year, month, biz_type
    ORDER BY year, month
""", conn)
df_monthly["ym"] = pd.to_datetime(df_monthly["year"] + "-" + df_monthly["month"])

# 제조업 대분류 전국
df_mfg = pd.read_sql("""
    SELECT year, month, SUM(power_usage) AS kwh
    FROM industry_type
    WHERE biz LIKE '%제조업%' AND metro='전체'
    AND year BETWEEN '2022' AND '2025'
    GROUP BY year, month ORDER BY year, month
""", conn)
df_mfg["ym"] = pd.to_datetime(df_mfg["year"] + "-" + df_mfg["month"])
conn.close()

# ── 분류
declining = df_annual[df_annual["rate_22_25"] < -5].sort_values("rate_22_25")
growing   = df_annual[df_annual["rate_22_25"] > 5].sort_values("rate_22_25", ascending=False)

# 핵심 업종 (절대량 감소가 큰 업종)
key_drop  = df_annual.sort_values("abs_change").head(8)
key_focus = ["영상. 음향", "1차   금속", "시  멘  트", "섬      유", "석유  정제", "화학  제품", "전기  기기", "자  동  차"]

COLORS = {
    "영상. 음향":  "#2563EB",
    "1차   금속":  "#DC2626",
    "시  멘  트":  "#D97706",
    "섬      유":  "#7C3AED",
    "석유  정제":  "#059669",
    "화학  제품":  "#DB2777",
    "전기  기기":  "#0891B2",
    "자  동  차":  "#84CC16",
}

fig = plt.figure(figsize=(18, 20))
fig.patch.set_facecolor("#F8FAFC")

def style_ax(ax, title):
    ax.set_facecolor("#FFFFFF")
    ax.set_title(title, fontsize=13, fontweight="bold", pad=10)
    ax.grid(axis="y", linestyle="--", alpha=0.4, color="#CBD5E1")
    ax.spines[["top", "right"]].set_visible(False)
    for s in ["left", "bottom"]:
        ax.spines[s].set_color("#94A3B8")

# ─────────────────────────────────────────────────
# [1] 업종별 22→25 증감률 수평 막대 (감소 상위)
# ─────────────────────────────────────────────────
ax1 = fig.add_subplot(4, 2, (1, 2))

# 주요 제조업종만 필터 (순수서비스·가정용 제외)
exclude = ["순수써비스", "가정용부문", "관  공  용", "기타공공용", "국  군  용", "유엔군  용", "농업. 임업", "어      업"]
df_bar = df_annual[~df_annual["biz_type"].isin(exclude)].sort_values("rate_22_25")

bar_colors = ["#DC2626" if v < 0 else "#16A34A" for v in df_bar["rate_22_25"]]
bars = ax1.barh(df_bar["biz_type"], df_bar["rate_22_25"], color=bar_colors, alpha=0.8, height=0.7)
ax1.axvline(0, color="#1E293B", lw=1)

for bar, val in zip(bars, df_bar["rate_22_25"]):
    x = val - 0.5 if val < 0 else val + 0.5
    ha = "right" if val < 0 else "left"
    ax1.text(x, bar.get_y() + bar.get_height()/2, f"{val:+.1f}%",
             va="center", ha=ha, fontsize=8.5, color="#1E293B")

ax1.set_xlabel("2022→2025 전력사용량 증감률 (%)", fontsize=10)
style_ax(ax1, "[1] 제조 업종별 전력사용량 증감률 (2022→2025)")

# ─────────────────────────────────────────────────
# [2] 핵심 업종 월별 추이 (반도체 vs 기타)
# ─────────────────────────────────────────────────
ax2 = fig.add_subplot(4, 1, 2)

for biz in key_focus:
    sub = df_monthly[df_monthly["biz_type"] == biz].set_index("ym")["kwh"]
    lw = 2.5 if biz == "영상. 음향" else 1.5
    ls = "-" if biz in ["영상. 음향", "1차   금속", "시  멘  트", "섬      유"] else "--"
    ax2.plot(sub.index, sub / 1e9, lw=lw, ls=ls,
             label=biz.strip(), color=COLORS.get(biz, "#94A3B8"), marker="o", ms=2)

for year in ["2023", "2024", "2025"]:
    ax2.axvline(pd.Timestamp(f"{year}-01-01"), color="#CBD5E1", lw=1, ls=":")
    ax2.text(pd.Timestamp(f"{year}-02-01"), ax2.get_ylim()[1] if ax2.get_ylim()[1] else 5,
             year, fontsize=9, color="#64748B")

ax2.set_ylabel("전력사용량 (십억 kWh)", fontsize=10)
ax2.legend(fontsize=9, ncol=4, loc="upper right")
style_ax(ax2, "[2] 주요 업종 월별 전력사용량 추이 (2022~2025)")

# ─────────────────────────────────────────────────
# [3] 영상.음향(반도체) YoY vs 1차금속(철강) YoY
# ─────────────────────────────────────────────────
ax3 = fig.add_subplot(4, 2, 5)

for biz, color in [("영상. 음향", "#2563EB"), ("1차   금속", "#DC2626"), ("시  멘  트", "#D97706"), ("섬      유", "#7C3AED")]:
    sub = df_monthly[df_monthly["biz_type"] == biz].set_index("ym")["kwh"]
    yoy = sub.pct_change(12) * 100
    ax3.plot(yoy.index, yoy, lw=1.8, label=biz.strip(), color=color)

ax3.axhline(0, color="#94A3B8", lw=1)
ax3.set_ylabel("전년 동월 대비 (%)", fontsize=10)
ax3.legend(fontsize=9)
style_ax(ax3, "[3] 주요 업종 YoY 증감률")

# ─────────────────────────────────────────────────
# [4] 절대 감소량 기여도 (2022→2025)
# ─────────────────────────────────────────────────
ax4 = fig.add_subplot(4, 2, 6)

df_contrib = df_annual[~df_annual["biz_type"].isin(exclude)].copy()
df_contrib = df_contrib[df_contrib["abs_change"] != 0].sort_values("abs_change")

colors4 = ["#DC2626" if v < 0 else "#16A34A" for v in df_contrib["abs_change"]]
ax4.barh(df_contrib["biz_type"], df_contrib["abs_change"], color=colors4, alpha=0.8, height=0.7)
ax4.axvline(0, color="#1E293B", lw=1)
ax4.set_xlabel("전력사용량 변화 (십억 kWh)", fontsize=10)
style_ax(ax4, "[4] 업종별 절대 감소량 기여 (2022→2025)")

# ─────────────────────────────────────────────────
# [5] 연간 합계 누적 막대 (감소 업종 vs 증가 업종)
# ─────────────────────────────────────────────────
ax5 = fig.add_subplot(4, 2, (7, 8))

top_decline = df_annual[~df_annual["biz_type"].isin(exclude)].sort_values("abs_change").head(6)["biz_type"].tolist()
top_grow    = df_annual[~df_annual["biz_type"].isin(exclude)].sort_values("abs_change", ascending=False).head(4)["biz_type"].tolist()
focus = top_decline + top_grow

years_list = ["2022", "2023", "2024", "2025"]
x = range(len(years_list))
palette = ["#DC2626","#EF4444","#F87171","#FCA5A5","#FECACA","#FEE2E2",
           "#16A34A","#22C55E","#4ADE80","#86EFAC"]

bottom_neg = [0]*4
bottom_pos = [0]*4
for i, biz in enumerate(focus):
    row = df_annual[df_annual["biz_type"] == biz]
    if row.empty: continue
    vals = [row[f"y{y[2:]}"].values[0] / 1e9 for y in years_list]
    color = palette[i % len(palette)]
    if vals[0] > 0:
        if i < 6:  # 감소 업종
            ax5.bar(list(x), [-v + vals[0] for v in vals],
                    bottom=bottom_neg, label=biz.strip(), color=color, alpha=0.85, width=0.5)
        else:
            ax5.bar([xi + 0.55 for xi in x], vals,
                    label=biz.strip()+" ↑", color=color, alpha=0.85, width=0.45)

# 전체 제조업 꺾은선
mfg_annual = df_mfg.groupby("year")["kwh"].sum() / 1e9
ax5_twin = ax5.twinx()
ax5_twin.plot(list(x), [mfg_annual.get(y, 0) for y in years_list],
              color="#1E293B", lw=2.5, marker="D", ms=7, label="제조업 전체", zorder=5)
ax5_twin.set_ylabel("제조업 전체 (십억 kWh)", fontsize=10)
ax5_twin.spines[["top"]].set_visible(False)

ax5.set_xticks(list(x))
ax5.set_xticklabels(years_list)
ax5.set_ylabel("전력사용량 (십억 kWh)", fontsize=10)

lines1, labels1 = ax5.get_legend_handles_labels()
lines2, labels2 = ax5_twin.get_legend_handles_labels()
ax5.legend(lines1 + lines2, labels1 + labels2, fontsize=8, ncol=3, loc="upper right")
style_ax(ax5, "[5] 주요 업종 연간 전력사용량 + 제조업 전체 추이")

# 연도 경계 재표시 ax2
for year in ["2023", "2024", "2025"]:
    ax2.axvline(pd.Timestamp(f"{year}-01-01"), color="#CBD5E1", lw=1, ls=":")

plt.suptitle("업종별 전력소비 분석 — 2025년 하락 원인 규명 (2022~2025)",
             fontsize=16, fontweight="bold", y=1.01, color="#1E293B")
plt.tight_layout(h_pad=3, w_pad=3)

import os
os.makedirs("analysis/output", exist_ok=True)
out = "analysis/output/industry_power_analysis.png"
plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"저장 완료: {out}")
