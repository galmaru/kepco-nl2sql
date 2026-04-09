"""
2025년 전력소비 하락 원인 규명 — 반도체 vs 전통제조업
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
from matplotlib import rcParams
import platform

if platform.system() == "Darwin":
    rcParams["font.family"] = "AppleGothic"
else:
    rcParams["font.family"] = "NanumGothic"
rcParams["axes.unicode_minus"] = False

conn = sqlite3.connect("data/kepco.db")

df_annual = pd.read_sql("""
    SELECT biz_type,
        SUM(CASE WHEN year='2022' THEN power_usage ELSE 0 END) AS y22,
        SUM(CASE WHEN year='2023' THEN power_usage ELSE 0 END) AS y23,
        SUM(CASE WHEN year='2024' THEN power_usage ELSE 0 END) AS y24,
        SUM(CASE WHEN year='2025' THEN power_usage ELSE 0 END) AS y25
    FROM business_type
    WHERE year BETWEEN '2022' AND '2025'
    GROUP BY biz_type HAVING y22>0 AND y25>0
""", conn)

df_monthly = pd.read_sql("""
    SELECT year, month, biz_type, SUM(power_usage) AS kwh
    FROM business_type
    WHERE year BETWEEN '2022' AND '2025'
    GROUP BY year, month, biz_type ORDER BY year, month
""", conn)
conn.close()

df_annual["rate"] = (df_annual["y25"] - df_annual["y22"]) / df_annual["y22"] * 100
df_annual["delta"] = (df_annual["y25"] - df_annual["y22"]) / 1e9  # 십억kWh
df_monthly["ym"] = pd.to_datetime(df_monthly["year"] + "-" + df_monthly["month"])

# 서비스·공공 제외 (제조·산업 집중)
exclude = ["순수써비스","가정용부문","관  공  용","기타공공용","국  군  용","유엔군  용","농업. 임업","어      업","전      철","수      도"]
df_mfg = df_annual[~df_annual["biz_type"].isin(exclude)].copy()

# 주요 업종 색상 (반도체 파랑, 나머지 감소는 붉은 계열, 증가는 초록)
COLOR_MAP = {
    "영상. 음향":  "#2563EB",   # 반도체 (파랑)
    "1차   금속":  "#B91C1C",   # 철강 (진빨)
    "시  멘  트":  "#D97706",   # 시멘트 (주황)
    "섬      유":  "#7C3AED",   # 섬유 (보라)
    "석유  정제":  "#065F46",   # 석유 (진초록)
    "화학  제품":  "#DB2777",   # 화학 (핑크)
    "조립  금속":  "#9F1239",   # 조립금속
    "고무. 플라":  "#92400E",   # 고무플라스틱
    "전기  기기":  "#0891B2",   # 전기기기 (청록)
    "자  동  차":  "#4D7C0F",   # 자동차 (연두)
}

# ══════════════════════════════════════════════════════════
fig, axes = plt.subplots(2, 2, figsize=(18, 14))
fig.patch.set_facecolor("#F8FAFC")
plt.suptitle("업종별 전력소비 분석 — 2025년 하락의 주범은 반도체인가?",
             fontsize=17, fontweight="bold", y=1.02, color="#1E293B")

def style(ax, title):
    ax.set_facecolor("#FFFFFF")
    ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    ax.grid(axis="y", ls="--", alpha=0.35, color="#CBD5E1")
    ax.spines[["top","right"]].set_visible(False)
    ax.spines[["left","bottom"]].set_color("#94A3B8")

# ─────────────────────────────────────────────────────────
# [좌상] 업종별 2022→2025 증감률 수평 막대
# ─────────────────────────────────────────────────────────
ax1 = axes[0, 0]
df_bar = df_mfg.sort_values("rate")
bar_colors = []
for biz in df_bar["biz_type"]:
    if biz in COLOR_MAP:
        bar_colors.append(COLOR_MAP[biz])
    elif df_bar.loc[df_bar["biz_type"]==biz,"rate"].values[0] < 0:
        bar_colors.append("#FCA5A5")
    else:
        bar_colors.append("#86EFAC")

bars = ax1.barh(df_bar["biz_type"].str.strip(), df_bar["rate"],
                color=bar_colors, alpha=0.9, height=0.65, edgecolor="white")
ax1.axvline(0, color="#374151", lw=1.2)

# 반도체 강조 박스
semi_idx = df_bar["biz_type"].str.strip().tolist().index("영상. 음향")
ax1.barh("영상. 음향", df_bar[df_bar["biz_type"]=="영상. 음향"]["rate"].values[0],
         color="#2563EB", alpha=1.0, height=0.65, edgecolor="#1D4ED8", linewidth=2)

for bar, val in zip(bars, df_bar["rate"]):
    x = val - 0.3 if val < 0 else val + 0.3
    ax1.text(x, bar.get_y()+bar.get_height()/2, f"{val:+.1f}%",
             va="center", ha="right" if val<0 else "left", fontsize=8.5, color="#1E293B")

ax1.set_xlabel("2022 → 2025 전력사용량 증감률 (%)", fontsize=10)
ax1.axvspan(-30, 0, alpha=0.03, color="#DC2626")
ax1.axvspan(0, 20, alpha=0.03, color="#16A34A")

# 반도체 주석
ax1.annotate("반도체·전자\n(영상·음향)\n-4.1%", xy=(-4.1, semi_idx),
             xytext=(-25, semi_idx+2),
             fontsize=9, color="#2563EB", fontweight="bold",
             arrowprops=dict(arrowstyle="->", color="#2563EB", lw=1.5))

style(ax1, "[1] 제조 업종별 전력사용량 증감률 (2022→2025)")

# ─────────────────────────────────────────────────────────
# [우상] 절대 감소량 기여 (십억 kWh)
# ─────────────────────────────────────────────────────────
ax2 = axes[0, 1]
df_delta = df_mfg.sort_values("delta")
d_colors = [COLOR_MAP.get(b, "#FCA5A5") if v<0 else "#86EFAC"
            for b, v in zip(df_delta["biz_type"], df_delta["delta"])]
ax2.barh(df_delta["biz_type"].str.strip(), df_delta["delta"],
         color=d_colors, alpha=0.9, height=0.65, edgecolor="white")
ax2.axvline(0, color="#374151", lw=1.2)

for bar, val in zip(ax2.patches, df_delta["delta"]):
    if abs(val) > 0.3:
        x = val - 0.05 if val < 0 else val + 0.05
        ax2.text(x, bar.get_y()+bar.get_height()/2, f"{val:+.1f}",
                 va="center", ha="right" if val<0 else "left", fontsize=8.5, color="#1E293B")

ax2.set_xlabel("전력사용량 변화 (십억 kWh)", fontsize=10)
total_drop = df_delta["delta"].sum()
ax2.set_title(f"[2] 업종별 절대 감소량 기여\n전체 합산: {total_drop:+.1f}십억 kWh",
              fontsize=13, fontweight="bold", pad=12)
ax2.set_facecolor("#FFFFFF")
ax2.grid(axis="x", ls="--", alpha=0.35, color="#CBD5E1")
ax2.spines[["top","right"]].set_visible(False)

# ─────────────────────────────────────────────────────────
# [좌하] 핵심 5개 업종 월별 추이
# ─────────────────────────────────────────────────────────
ax3 = axes[1, 0]
focus_biz = ["영상. 음향", "1차   금속", "시  멘  트", "섬      유", "전기  기기", "자  동  차"]

for biz in focus_biz:
    sub = df_monthly[df_monthly["biz_type"]==biz].set_index("ym")["kwh"] / 1e9
    lw = 2.8 if biz == "영상. 음향" else 1.8
    ax3.plot(sub.index, sub, lw=lw, label=biz.strip(),
             color=COLOR_MAP.get(biz, "#94A3B8"), marker="o" if biz=="영상. 음향" else None, ms=2.5)

for year in ["2023","2024","2025"]:
    ax3.axvline(pd.Timestamp(f"{year}-01-01"), color="#CBD5E1", lw=1, ls=":")
    ylim = ax3.get_ylim()
    ax3.text(pd.Timestamp(f"{year}-02-01"), ylim[1]*0.98 if ylim[1] else 5,
             year, fontsize=9, color="#64748B")

ax3.set_ylabel("전력사용량 (십억 kWh)", fontsize=10)
ax3.legend(fontsize=9.5, loc="lower left", ncol=2)
style(ax3, "[3] 핵심 업종 월별 전력사용량 추이")

# ─────────────────────────────────────────────────────────
# [우하] YoY 증감률 비교 (반도체 vs 하락 주범)
# ─────────────────────────────────────────────────────────
ax4 = axes[1, 1]
yoy_targets = [
    ("영상. 음향", "#2563EB", 2.8, "-"),
    ("1차   금속", "#B91C1C", 1.8, "--"),
    ("시  멘  트", "#D97706", 1.8, "--"),
    ("섬      유", "#7C3AED", 1.8, ":"),
]
for biz, color, lw, ls in yoy_targets:
    sub = df_monthly[df_monthly["biz_type"]==biz].set_index("ym")["kwh"]
    yoy = sub.pct_change(12)*100
    ax4.plot(yoy.index, yoy, lw=lw, ls=ls, label=biz.strip(), color=color)

ax4.axhline(0, color="#374151", lw=1.2)
ax4.fill_between(yoy.index, 0, -50, alpha=0.04, color="#DC2626")
ax4.fill_between(yoy.index, 0,  30, alpha=0.04, color="#16A34A")

# 주요 이벤트 주석
ax4.annotate("반도체 감산\n(HBM 전환)", xy=(pd.Timestamp("2023-05-01"), -7),
             xytext=(pd.Timestamp("2023-02-01"), -22),
             fontsize=8.5, color="#2563EB",
             arrowprops=dict(arrowstyle="->", color="#2563EB"))
ax4.annotate("HBM 반등", xy=(pd.Timestamp("2024-08-01"), 5),
             xytext=(pd.Timestamp("2024-04-01"), 18),
             fontsize=8.5, color="#2563EB",
             arrowprops=dict(arrowstyle="->", color="#2563EB"))

ax4.set_ylabel("전년 동월 대비 (%)", fontsize=10)
ax4.set_ylim(-45, 35)
ax4.legend(fontsize=9.5, loc="lower right")
style(ax4, "[4] 전년 동월 대비 증감률 (YoY) 비교")
ax4.grid(axis="y", ls="--", alpha=0.35, color="#CBD5E1")

# 전체 레이아웃 마무리
semi_patch = mpatches.Patch(color="#2563EB", label="반도체·전자 (영상·음향)")
decline_patch = mpatches.Patch(color="#B91C1C", label="전통 제조업 (철강·시멘트·섬유)")
fig.legend(handles=[semi_patch, decline_patch], loc="lower center", ncol=2,
           fontsize=11, framealpha=0.9, bbox_to_anchor=(0.5, -0.02))

plt.tight_layout(h_pad=4, w_pad=3)
out = "analysis/output/industry_decline_2025.png"
plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"저장: {out}")
