"""TikTok platform adapter using TikTok Content Posting API v2.

Video-only — TikTok's Content Posting API does not support images.
Flow: init upload → upload video chunks → publish with caption.
"""
from __future__ import annotations

import logging
import os
import time

import requests

from src.services.social.base import PostResult, SocialPlatformAdapter

logger = logging.getLogger(__name__)

_INIT_URL = "https://open.tiktokapis.com/v2/post/publish/video/init/"
_PUBLISH_URL = "https://open.tiktokapis.com/v2/post/publish/status/fetch/"
_DIRECT_POST_URL = "https://open.tiktokapis.com/v2/post/publish/video/init/"


class TikTokAdapter(SocialPlatformAdapter):
    platform_id = "tiktok"

    def __init__(self):
        self._access_token = os.environ.get("TIKTOK_ACCESS_TOKEN", "")
        if not self._access_token:
            raise ValueError("TIKTOK_ACCESS_TOKEN env var not set")

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=UTF-8",
        }

    def post_text(self, caption: str) -> PostResult:
        """TikTok doesn't support text-only posts."""
        return PostResult(
            platform="tiktok",
            success=False,
            error="TikTok requires video content — text-only posts not supported",
        )

    def post_with_media(self, caption: str, media_url: str, media_type: str) -> PostResult:
        """Post video to TikTok via Content Posting API.

        Uses pull-from-URL method — TikTok fetches the video from our presigned S3 URL.
        """
        if media_type != "video":
            return PostResult(
                platform="tiktok",
                success=False,
                error=f"TikTok only supports video posts, got media_type={media_type}",
            )

        try:
            # Use direct post with pull_from_url source
            payload = {
                "post_info": {
                    "title": caption[:150],  # TikTok title max ~150 chars
                    "privacy_level": "SELF_ONLY",  # Start private, can change later
                    "disable_duet": False,
                    "disable_comment": False,
                    "disable_stitch": False,
                },
                "source_info": {
                    "source": "PULL_FROM_URL",
                    "video_url": media_url,
                },
            }

            resp = requests.post(
                _DIRECT_POST_URL,
                headers=self._headers(),
                json=payload,
                timeout=30,
            )

            if resp.status_code == 200:
                data = resp.json().get("data", {})
                publish_id = data.get("publish_id", "")
                logger.info("TikTok post initiated publish_id=%s", publish_id)

                # Poll for status (TikTok processes async)
                status = self._poll_status(publish_id)
                if status == "PUBLISH_COMPLETE":
                    return PostResult(
                        platform="tiktok",
                        success=True,
                        platform_post_id=publish_id,
                    )
                else:
                    return PostResult(
                        platform="tiktok",
                        success=False,
                        error=f"TikTok publish status: {status}",
                        platform_post_id=publish_id,
                    )
            else:
                error_data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                error_msg = error_data.get("error", {}).get("message", resp.text[:200])
                logger.error("TikTok post failed: %s %s", resp.status_code, error_msg)
                return PostResult(
                    platform="tiktok",
                    success=False,
                    error=f"{resp.status_code}: {error_msg}",
                )

        except Exception as exc:
            logger.error("TikTok adapter error: %s", exc)
            return PostResult(platform="tiktok", success=False, error=str(exc))

    def _poll_status(self, publish_id: str, max_attempts: int = 5) -> str:
        """Poll TikTok for publish status. Returns status string."""
        for attempt in range(max_attempts):
            time.sleep(3)
            try:
                resp = requests.post(
                    _PUBLISH_URL,
                    headers=self._headers(),
                    json={"publish_id": publish_id},
                    timeout=10,
                )
                if resp.status_code == 200:
                    status = resp.json().get("data", {}).get("status", "PROCESSING")
                    logger.info("TikTok status poll %d: %s", attempt + 1, status)
                    if status in ("PUBLISH_COMPLETE", "FAILED"):
                        return status
            except Exception as exc:
                logger.warning("TikTok status poll failed: %s", exc)

        return "PROCESSING_TIMEOUT"
