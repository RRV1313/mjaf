## Pipelines Currently Managed by algo features
```mermaid
---
title: user_features
---
graph
silver_user_check_partition ---> silver_user_daily_rollup_default_action
silver_user_check_partition ---> trial_features_default_action
silver_user_daily_rollup_default_action ---> user_features_default_action
silver_video_all_time_rollup_default_action ---> user_features_default_action
silver_video_check_partition ---> silver_video_daily_rollup_default_action
silver_video_daily_rollup_agg_default_action ---> user_features_default_action
silver_video_daily_rollup_default_action ---> silver_video_all_time_rollup_default_action
silver_video_daily_rollup_default_action ---> silver_video_daily_rollup_agg_default_action
silver_video_daily_rollup_default_action ---> user_features_default_action
trial_features_default_action ---> user_features_default_action

```
