# youtube_livechat_crawler

## Requirements

- Create Service Account  
   ex. `youtube-livechat-crawler@<PROJECT_ID>.iam.gserviceaccount.com`

- Create Cloud Storage Bucket  
   ex. `youtube-livechat-crawler`

## How to deploy

### `search_channel_videos`

```sh
cd search_channel_videos
gcloud builds submit --substitutions=_SERVICE_ACCOUNT=<SERVICE_ACCOUNT>
```

### `check_untouched_video`

```sh
cd check_untouched_video
gcloud builds submit --substitutions=_SERVICE_ACCOUNT=<SERVICE_ACCOUNT>
```

### `crawl_livechat`

```sh
cd crawl_livechat
gcloud builds submit --substitutions=_SERVICE_ACCOUNT=<SERVICE_ACCOUNT>
```
