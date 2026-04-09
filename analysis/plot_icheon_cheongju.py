"""
경기 이천시 vs 충북 청주시 월별 전력소비 추이 분석
- 전체 계약종 합산 전력사용량
- 2022년 1월 ~ 2025년 12월
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib import rcParams
import platform

# 한글 폰트 설정
if platform.system() == "Darwin":
    rcParams["font.family"] = "AppleGothic"
else:
    rcParams["font.family"] = "NanumGothic"
rcParams["axes.unicode_minus"] = False

conn = sqlite3.connect("data/kepco.db")

query = """
SELECT year, month, metro, city, SUM(power_usage) AS total_kwh
FROM contract_type
WHERE city IN ('이천시', '청주시')
  AND metro != '전체' AND city != '전체'
  AND year BETWEEN '2022' AND '2026'
GROUP BY year, month, metro, city
ORDER BY year, month
"""
df = pd.read_sql(query, conn)
conn.close()

df["ym"] = pd.to_datetime(df["year"] + "-" + df["month"])
df["label"] = df.apply(lambda r: f"{r['metro'][:2]} {r['city']}", axis=1)

icheon  = df[df["city"] == "이천시"].set_index("ym")["total_kwh"]
cheongju = df[df["city"] == "청주시"].set_index("ym")["total_kwh"]

# ── 색상 팔레트
C_IC = "#2563EB"   # 이천 파랑
C_CJ = "#DC2626"   # 청주 빨강
C_IC_L = "#BFDBFE"
C_CJ_L = "#FECACA"

fig = plt.figure(figsize=(16, 18))
fig.patch.set_facecolor("#F8FAFC")

# ── 공통 배경 함수
def style_ax(ax, title):
    ax.set_facecolor("#FFFFFF")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=10)
    ax.grid(axis="y", linestyle="--", alpha=0.4, color="#CBD5E1")
    ax.spines[["top","right"]].set_visible(False)
    for spine in ["left","bottom"]:
        ax.spines[spine].set_color("#94A3B8")

# ──────────────────────────────────────────────────────────
# [1] 월별 전력사용량 추이 (전체)
# ──────────────────────────────────────────────────────────
ax1 = fig.add_subplot(4, 1, 1)

ax1.fill_between(icheon.index,  icheon/1e6,  alpha=0.15, color=C_IC)
ax1.fill_between(cheongju.index, cheongju/1e6, alpha=0.15, color=C_CJ)
ax1.plot(icheon.index,  icheon/1e6,  color=C_IC,  lw=2, label="경기 이천시",  marker="o", ms=3)
ax1.plot(cheongju.index, cheongju/1e6, color=C_CJ, lw=2, label="충북 청주시", marker="s", ms=3)

# 연도 경계 표시
for year in ["2023","2024","2025"]:
    ax1.axvline(pd.Timestamp(f"{year}-01-01"), color="#94A3B8", lw=1, ls=":")
    ax1.text(pd.Timestamp(f"{year}-01-15"), ax1.get_ylim()[1]*0.97 if ax1.get_ylim()[1] else 1,
             year, fontsize=9, color="#64748B")

ax1.set_ylabel("전력사용량 (백만 kWh)", fontsize=11)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{x:,.0f}"))
ax1.legend(fontsize=11, loc="upper right")
style_ax(ax1, "[1] 월별 전력사용량 추이 (2022~2025)")

# ──────────────────────────────────────────────────────────
# [2] 연도별 연간 합산 비교 (막대)
# ──────────────────────────────────────────────────────────
ax2 = fig.add_subplot(4, 2, 3)

years = ["2022","2023","2024","2025","2026"]
ic_annual  = df[df["city"]=="이천시"].groupby("year")["total_kwh"].sum() / 1e9
cj_annual  = df[df["city"]=="청주시"].groupby("year")["total_kwh"].sum() / 1e9

x = range(len(years))
w = 0.35
bars1 = ax2.bar([i-w/2 for i in x], [ic_annual.get(y,0) for y in years], w,
                label="경기 이천시", color=C_IC, alpha=0.85)
bars2 = ax2.bar([i+w/2 for i in x], [cj_annual.get(y,0) for y in years], w,
                label="충북 청주시", color=C_CJ, alpha=0.85)

for bar in list(bars1)+list(bars2):
    ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.05,
             f"{bar.get_height():.1f}", ha="center", va="bottom", fontsize=9)

ax2.set_xticks(list(x)); ax2.set_xticklabels(years)
ax2.set_ylabel("연간 사용량 (십억 kWh)")
ax2.legend(fontsize=9)
style_ax(ax2, "[2] 연간 전력사용량 비교")

# ──────────────────────────────────────────────────────────
# [3] 전년 동월 대비 증감률 (YoY)
# ──────────────────────────────────────────────────────────
ax3 = fig.add_subplot(4, 2, 4)

ic_yoy  = icheon.pct_change(12)  * 100
cj_yoy  = cheongju.pct_change(12) * 100

ax3.axhline(0, color="#94A3B8", lw=1)
ax3.fill_between(ic_yoy.index,  ic_yoy,  0, where=ic_yoy>=0,  alpha=0.3, color=C_IC)
ax3.fill_between(ic_yoy.index,  ic_yoy,  0, where=ic_yoy<0,   alpha=0.3, color="#93C5FD")
ax3.fill_between(cj_yoy.index, cj_yoy,  0, where=cj_yoy>=0,  alpha=0.3, color=C_CJ)
ax3.fill_between(cj_yoy.index, cj_yoy,  0, where=cj_yoy<0,   alpha=0.3, color="#FCA5A5")
ax3.plot(ic_yoy.index,  ic_yoy,  color=C_IC,  lw=1.5, label="이천시")
ax3.plot(cj_yoy.index, cj_yoy, color=C_CJ, lw=1.5, label="청주시")
ax3.set_ylabel("전년 동월 대비 (%)")
ax3.legend(fontsize=9)
style_ax(ax3, "[3] 전년 동월 대비 증감률 (YoY)")

# ──────────────────────────────────────────────────────────
# [4] 계약종별 비중 (이천 vs 청주, 2022~2025 합산)
# ──────────────────────────────────────────────────────────
conn2 = sqlite3.connect("data/kepco.db")
df_cntr = pd.read_sql("""
    SELECT city, contract_type, SUM(power_usage) AS kwh
    FROM contract_type
    WHERE city IN ('이천시','청주시') AND metro!='전체' AND city!='전체'
      AND year BETWEEN '2022' AND '2026'
    GROUP BY city, contract_type
    ORDER BY kwh DESC
""", conn2)
conn2.close()

for idx, (city, color, ax_n) in enumerate([("이천시", C_IC, 5), ("청주시", C_CJ, 6)]):
    ax_pie = fig.add_subplot(4, 2, ax_n)
    sub = df_cntr[df_cntr["city"]==city].sort_values("kwh", ascending=False)
    wedge_colors = [f"{color}CC","#60A5FA","#F87171","#34D399","#FBBF24","#A78BFA","#94A3B8"]
    wedges, texts, autotexts = ax_pie.pie(
        sub["kwh"], labels=sub["contract_type"],
        autopct="%1.1f%%", startangle=90,
        colors=wedge_colors[:len(sub)],
        wedgeprops={"edgecolor":"white","linewidth":1.5},
        pctdistance=0.75
    )
    for t in autotexts: t.set_fontsize(9)
    for t in texts: t.set_fontsize(9)
    ax_pie.set_title(f"[{4+idx+1}] {city} 계약종별 비중\n(2022~2026 합산)", fontsize=12, fontweight="bold")

# ──────────────────────────────────────────────────────────
# [6] 월별 계절 패턴 (연도별 선)
# ──────────────────────────────────────────────────────────
ax6 = fig.add_subplot(4, 2, 7)
palette_ic = ["#1D4ED8","#3B82F6","#60A5FA","#93C5FD","#BFDBFE"]
for i, year in enumerate(years):
    sub = df[(df["city"]=="이천시") & (df["year"]==year)].sort_values("month")
    if not sub.empty:
        ax6.plot(sub["month"].astype(int), sub["total_kwh"]/1e6,
                 lw=2, label=year, color=palette_ic[i], marker="o", ms=4)
ax6.set_xlabel("월"); ax6.set_ylabel("백만 kWh")
ax6.set_xticks(range(1,13)); ax6.legend(fontsize=9, ncol=2)
style_ax(ax6, "[6] 이천시 월별 계절 패턴")

ax7 = fig.add_subplot(4, 2, 8)
palette_cj = ["#991B1B","#DC2626","#EF4444","#FCA5A5","#FECACA"]
for i, year in enumerate(years):
    sub = df[(df["city"]=="청주시") & (df["year"]==year)].sort_values("month")
    if not sub.empty:
        ax7.plot(sub["month"].astype(int), sub["total_kwh"]/1e6,
                 lw=2, label=year, color=palette_cj[i], marker="s", ms=4)
ax7.set_xlabel("월"); ax7.set_ylabel("백만 kWh")
ax7.set_xticks(range(1,13)); ax7.legend(fontsize=9, ncol=2)
style_ax(ax7, "[7] 청주시 월별 계절 패턴")

# ── 연도 경계 재표시 (ax1)
for year in ["2023","2024","2025"]:
    ax1.axvline(pd.Timestamp(f"{year}-01-01"), color="#94A3B8", lw=1, ls=":")
    ylim = ax1.get_ylim()
    ax1.text(pd.Timestamp(f"{year}-02-01"), ylim[1]*0.97, year, fontsize=9, color="#64748B")

plt.suptitle("경기 이천시 vs 충북 청주시 — 전력소비 심층 분석 (2022~2026.01)",
             fontsize=16, fontweight="bold", y=1.01, color="#1E293B")
plt.tight_layout(h_pad=3)

import os
os.makedirs("analysis/output", exist_ok=True)
out = "analysis/output/icheon_cheongju_power.png"
plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"저장 완료: {out}")
plt.show()
