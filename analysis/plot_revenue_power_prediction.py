import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib import rcParams
import platform
import os

# 한글 폰트 설정
if platform.system() == "Darwin":
    rcParams["font.family"] = "AppleGothic"
else:
    rcParams["font.family"] = "NanumGothic"
rcParams["axes.unicode_minus"] = False

# 1. 삼성전자 DS, SK하이닉스 매출 데이터 (단위: 조 원)
data = {
    "year": [2022, 2023, 2024, 2025],
    "samsung_rev": [98.5, 66.6, 110.0, 130.0],
    "hynix_rev": [44.6, 32.8, 66.2, 97.2]
}
df_rev = pd.DataFrame(data)
df_rev["total_rev"] = df_rev["samsung_rev"] + df_rev["hynix_rev"]

# 2. DB에서 반도체(영상.음향) 산업 전력 사용량 가져오기
conn = sqlite3.connect("data/kepco.db")
query = """
SELECT year, SUM(power_usage) AS total_kwh
FROM business_type
WHERE biz_type = '영상. 음향'
  AND year IN ('2022', '2023', '2024', '2025')
GROUP BY year
ORDER BY year
"""
df_power = pd.read_sql(query, conn)
conn.close()

df_power["year"] = df_power["year"].astype(int)
df_power["power_twh"] = df_power["total_kwh"] / 1e9  # 십억 kWh (TWh)

# 데이터 병합
df = pd.merge(df_rev, df_power, on="year")

# 3. 선형 회귀 분석 (매출 -> 전력수요 예측)
# 전력 사용량은 기본적으로 깔려있는 베이스라인이 있고, 매출 증가에 따라 가동률이 올라가며 추가 전력을 사용함.
x = df["total_rev"].values
y = df["power_twh"].values
coef = np.polyfit(x, y, 1)
poly1d_fn = np.poly1d(coef)

# 2026, 2027년 매출 시나리오 가정 (AI 사이클 지속 가정: 2026년 15% 성장, 2027년 10% 성장)
rev_2026 = df.loc[df["year"]==2025, "total_rev"].values[0] * 1.15
rev_2027 = rev_2026 * 1.10

pred_power_2026 = poly1d_fn(rev_2026)
pred_power_2027 = poly1d_fn(rev_2027)

# 예측 데이터를 데이터프레임에 추가
future_data = pd.DataFrame({
    "year": [2026, 2027],
    "samsung_rev": [np.nan, np.nan],
    "hynix_rev": [np.nan, np.nan],
    "total_rev": [rev_2026, rev_2027],
    "total_kwh": [np.nan, np.nan],
    "power_twh": [pred_power_2026, pred_power_2027],
    "is_pred": [True, True]
})
df["is_pred"] = False
df_all = pd.concat([df, future_data], ignore_index=True)

# 4. 시각화
fig = plt.figure(figsize=(15, 10))
fig.patch.set_facecolor("#F8FAFC")

# 공통 축 스타일
def style_ax(ax, title):
    ax.set_facecolor("#FFFFFF")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=15)
    ax.grid(axis="y", linestyle="--", alpha=0.4, color="#CBD5E1")
    ax.spines[["top","right"]].set_visible(False)
    for spine in ["left","bottom"]:
        ax.spines[spine].set_color("#94A3B8")

# [그래프 1] 반도체 매출 vs 전력 사용량 산점도 및 회귀선
ax1 = fig.add_subplot(2, 2, 1)
ax1.scatter(df["total_rev"], df["power_twh"], color="#2563EB", s=100, label="실제 데이터 (22~25)", zorder=5)
ax1.scatter(future_data["total_rev"], future_data["power_twh"], color="#DC2626", s=100, marker="*", label="예측 (26~27)", zorder=5)

x_line = np.linspace(50, 300, 100)
ax1.plot(x_line, poly1d_fn(x_line), color="#94A3B8", linestyle="--", label=f"추세선: y = {coef[0]:.2f}x + {coef[1]:.1f}")

for i, row in df_all.iterrows():
    ax1.text(row["total_rev"], row["power_twh"] + 0.5, str(int(row["year"])), fontsize=10, ha="center", color="#1E293B")

ax1.set_xlabel("삼성+SK 반도체 합산 매출 (조 원)", fontsize=11)
ax1.set_ylabel("전력 사용량 (TWh)", fontsize=11)
ax1.legend(fontsize=10)
style_ax(ax1, "[1] 합산 매출과 전력 사용량의 상관관계")

# [그래프 2] 연도별 매출 및 전력수요 예측 추이
ax2 = fig.add_subplot(2, 2, (3, 4))

x_pos = np.arange(len(df_all["year"]))
w = 0.35

# 매출 막대 (왼쪽 축)
bars_rev = ax2.bar(x_pos - w/2, df_all["total_rev"], w, color="#3B82F6", alpha=0.8, label="합산 매출 (조 원)")
# 예측 년도 빗금 패턴 처리
bars_rev[4].set_hatch('//')
bars_rev[5].set_hatch('//')
bars_rev[4].set_alpha(0.5)
bars_rev[5].set_alpha(0.5)

ax2.set_ylabel("반도체 합산 매출 (조 원)", fontsize=11, color="#1D4ED8")
ax2.tick_params(axis="y", colors="#1D4ED8")

# 전력 막대 (오른쪽 축)
ax2_twin = ax2.twinx()
bars_power = ax2_twin.bar(x_pos + w/2, df_all["power_twh"], w, color="#10B981", alpha=0.8, label="전력 사용량 (TWh)")
bars_power[4].set_hatch('//')
bars_power[5].set_hatch('//')
bars_power[4].set_alpha(0.5)
bars_power[5].set_alpha(0.5)

ax2_twin.set_ylabel("전력 사용량 (TWh)", fontsize=11, color="#047857")
ax2_twin.tick_params(axis="y", colors="#047857")
ax2_twin.spines[["top", "left"]].set_visible(False)
ax2_twin.spines["right"].set_color("#94A3B8")

ax2.set_xticks(x_pos)
ax2.set_xticklabels([f"{int(y)}\n(예측)" if p else str(int(y)) for y, p in zip(df_all["year"], df_all["is_pred"])])

# 값 텍스트 표시
for i, bar in enumerate(bars_rev):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3, f"{bar.get_height():.1f}조", ha="center", va="bottom", fontsize=9, color="#1D4ED8")
for i, bar in enumerate(bars_power):
    ax2_twin.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, f"{bar.get_height():.1f}TWh", ha="center", va="bottom", fontsize=9, color="#047857")

# 범례 통합
lines, labels = ax2.get_legend_handles_labels()
lines2, labels2 = ax2_twin.get_legend_handles_labels()
ax2.legend(lines + lines2, labels + labels2, loc="upper left", fontsize=10)
style_ax(ax2, "[2] 연도별 반도체 매출 및 전력 수요 예측 (2022~2027)")

plt.suptitle("삼성전자·SK하이닉스 매출 성장에 따른 반도체 산업 전력 수요 예측", fontsize=16, fontweight="bold", y=0.98, color="#1E293B")
plt.tight_layout(pad=3)

# 저장
os.makedirs("analysis/output", exist_ok=True)
out_path = "analysis/output/semiconductor_power_prediction.png"
plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"저장 완료: {out_path}")
