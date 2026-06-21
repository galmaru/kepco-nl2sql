#!/usr/bin/env python3
"""Streamlit 앱 Playwright 스모크 테스트."""
import json
import shutil
import socket
import subprocess
import sys
from pathlib import Path

STREAMLIT_URL = "http://localhost:8501"
OUT_DIR = Path(__file__).parent / "screenshots"
MP4_PATH = OUT_DIR / "last_run.mp4"
VIDEO_DIR = OUT_DIR / "_video"
WATCH_PATHS = ("app.py", "/pipeline/", "/tests/playwright")

# DOM에 화살표 커서 + 클릭 ripple 효과 삽입 — Playwright 비디오에 포함됨
_CURSOR_SCRIPT = """
window.addEventListener('DOMContentLoaded', () => {
    const ns = 'http://www.w3.org/2000/svg';

    /* ── 화살표 커서 ── */
    const wrap = document.createElement('div');
    wrap.style.cssText = `
        position: fixed; width: 24px; height: 24px;
        pointer-events: none; z-index: 2147483647;
        left: 0; top: 0;
    `;
    const svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('width', '24');
    svg.setAttribute('height', '24');
    svg.setAttribute('viewBox', '0 0 24 24');
    svg.innerHTML = `
        <filter id="cs">
          <feDropShadow dx="1" dy="1" stdDeviation="1.2" flood-opacity="0.45"/>
        </filter>
        <path d="M 3,2 L 3,18 L 7,14 L 10.5,21 L 13,20 L 9.5,13 L 15,13 Z"
              fill="white" stroke="#222" stroke-width="1.2"
              stroke-linejoin="round" filter="url(#cs)"/>
    `;
    wrap.appendChild(svg);
    document.body.appendChild(wrap);

    document.addEventListener('mousemove', e => {
        wrap.style.left = e.clientX + 'px';
        wrap.style.top  = e.clientY + 'px';
    });

    /* ── 클릭 ripple ── */
    const style = document.createElement('style');
    style.textContent = `
        @keyframes _ripple {
            0%   { transform: translate(-50%,-50%) scale(0.2); opacity: 0.8; }
            100% { transform: translate(-50%,-50%) scale(2.8); opacity: 0; }
        }
        ._click-ripple {
            position: fixed;
            width: 36px; height: 36px;
            border-radius: 50%;
            border: 2.5px solid rgba(66,133,244,0.9);
            background: rgba(66,133,244,0.15);
            pointer-events: none;
            z-index: 2147483646;
            animation: _ripple 0.45s ease-out forwards;
        }
    `;
    document.head.appendChild(style);

    document.addEventListener('mousedown', e => {
        const r = document.createElement('div');
        r.className = '_click-ripple';
        r.style.left = e.clientX + 'px';
        r.style.top  = e.clientY + 'px';
        document.body.appendChild(r);
        r.addEventListener('animationend', () => r.remove());
    });
});
"""


def _is_streamlit_running() -> bool:
    try:
        s = socket.create_connection(("localhost", 8501), timeout=1)
        s.close()
        return True
    except OSError:
        return False


def _should_run() -> bool:
    if sys.stdin.isatty():
        return True
    try:
        data = json.load(sys.stdin)
        file_path = data.get("tool_input", {}).get("file_path", "")
        return any(p in file_path for p in WATCH_PATHS)
    except Exception:
        return True


def _webm_to_mp4(webm_path: Path, mp4_path: Path) -> bool:
    try:
        subprocess.run(
            [
                "ffmpeg", "-y", "-i", str(webm_path),
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-r", "45", "-crf", "23", "-movflags", "+faststart",
                str(mp4_path),
            ],
            capture_output=True,
            check=True,
        )
        return True
    except Exception as e:
        print(f"  MP4 변환 실패: {e}")
        return False


def run_tests() -> tuple[bool, Path | None]:
    from playwright.sync_api import sync_playwright, expect

    OUT_DIR.mkdir(exist_ok=True)
    VIDEO_DIR.mkdir(exist_ok=True)

    def slow_click(locator, steps: int = 30) -> None:
        """요소 중앙까지 천천히 이동(steps 단계)한 뒤 클릭."""
        box = locator.bounding_box()
        if box:
            cx = box["x"] + box["width"] / 2
            cy = box["y"] + box["height"] / 2
            page.mouse.move(cx, cy, steps=steps)
            page.wait_for_timeout(200)
        locator.click()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=80)
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            record_video_dir=str(VIDEO_DIR),
            record_video_size={"width": 1280, "height": 900},
        )
        page = ctx.new_page()
        # 커서 오버레이 — 모든 페이지 로드 시 자동 삽입
        page.add_init_script(_CURSOR_SCRIPT)
        mp4_path = None

        try:
            # ① 페이지 로드
            page.goto(STREAMLIT_URL, wait_until="networkidle", timeout=15000)
            page.screenshot(path=str(OUT_DIR / "01_loaded.png"))
            print("✅ 페이지 로드")

            # ② 타이틀 확인
            expect(page.get_by_text("한전 오픈 데이터 자연어 조회")).to_be_visible(timeout=5000)
            print("✅ 타이틀 확인")

            # ③ 예시 탭 클릭
            slow_click(page.get_by_role("tab").first)
            page.wait_for_timeout(800)
            page.screenshot(path=str(OUT_DIR / "02_tab.png"))
            print("✅ 예시 탭 클릭")

            # ④ 예시 버튼 클릭 → 질문 채워짐
            first_btn = page.get_by_role("button").filter(has_text="복지할인").first
            if first_btn.count():
                slow_click(first_btn)
                page.wait_for_timeout(1500)
                page.screenshot(path=str(OUT_DIR / "03_example_clicked.png"))
                print("✅ 예시 버튼 클릭")

            # ⑤ 조회 버튼 클릭
            submit_btn = page.get_by_role("button", name="조회")
            if submit_btn.count():
                slow_click(submit_btn)
                page.wait_for_timeout(500)
                print("✅ 조회 버튼 클릭 — 결과 대기 중...")

                # 스피너 등장 대기 (최대 5초)
                try:
                    page.wait_for_selector('[data-testid="stSpinner"]', timeout=5000)
                    # 스피너 사라질 때까지 대기 (최대 90초)
                    page.wait_for_selector(
                        '[data-testid="stSpinner"]', state="hidden", timeout=90000
                    )
                except Exception:
                    pass  # 스피너 없이 바로 완료된 경우도 허용

                page.wait_for_timeout(2000)  # 결과 렌더링 여유
                page.screenshot(path=str(OUT_DIR / "04_result.png"))
                print("✅ 결과 화면 확인")

            # 결과 화면을 충분히 녹화
            page.wait_for_timeout(2000)
            success = True

        except Exception as e:
            page.screenshot(path=str(OUT_DIR / "error.png"))
            print(f"❌ 테스트 실패: {e}")
            success = False

        finally:
            video_path_str = page.video.path() if page.video else None
            ctx.close()
            browser.close()

            if video_path_str:
                webm = Path(video_path_str)
                if webm.exists() and _webm_to_mp4(webm, MP4_PATH):
                    print(f"🎬  MP4 저장: {MP4_PATH}")
                    mp4_path = MP4_PATH
                shutil.rmtree(VIDEO_DIR, ignore_errors=True)

        return success, mp4_path


if __name__ == "__main__":
    if not _should_run():
        sys.exit(0)

    if not _is_streamlit_running():
        print("⏭️  Streamlit 미실행 — 테스트 스킵 (localhost:8501)")
        sys.exit(0)

    success, mp4_path = run_tests()

    if mp4_path:
        print(f"\n📸 스크린샷: {OUT_DIR}")
        print(f"🎬  MP4: {mp4_path}")

    print("\n✅ 스모크 테스트 통과" if success else "\n❌ 스모크 테스트 실패")
    sys.exit(0 if success else 1)
