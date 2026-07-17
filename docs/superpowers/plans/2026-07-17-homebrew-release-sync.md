# Homebrew Release Sync

**Date:** 2026-07-17
**Status:** Implemented on `feature/homebrew-release-sync`

## 现状

- Housegate 当前正式 release 是 [`v0.6.0`](https://github.com/housegate/housegate/releases/tag/v0.6.0)，包含 `darwin-arm64`、`linux-amd64` 和 `SHA256SUMS`。
- tap 的 [`Formula/housegate.rb`](https://github.com/housegate/homebrew-housegate/blob/7849edaf929823e26b06583d60c877d145e36d8d/Formula/housegate.rb) 仍是 `v0.3.0`；需同步显式 `version`、两条 URL、两条 `sha256`、`install` 中的文件名和 `test` 的版本输出。
- 此前 [`release.yml`](../../../.github/workflows/release.yml) 已在创建 GitHub Release 前生成 `release/SHA256SUMS`，但没有更新 tap。Homebrew 官方也要求版本更新至少同步 URL 和 SHA-256：[Updating Software](https://docs.brew.sh/Updating-Software-in-Homebrew)。

## 已实现方案

[`release.yml`](../../../.github/workflows/release.yml) 保留原有 `release` job 负责 tag、构建、GHCR 和 GitHub Release，并通过独立的 `homebrew` job 调用可复用的 [`sync-homebrew.yml`](../../../.github/workflows/sync-homebrew.yml)：

1. `release` 从本次生成的 `SHA256SUMS` 导出 exact tag、Darwin SHA 和 Linux SHA。
2. `homebrew` 通过 `needs: release` 消费这些值，不查询 `/latest`。
3. job 重新下载 exact-tag assets 并核对两份 checksum。
4. [`update_homebrew_formula.rb`](../../../.github/scripts/update_homebrew_formula.rb) 更新 `version`、两条 URL、两条 `sha256`、`install` 文件名和 `test` 期望输出。
5. updater 拒绝非正式 tag、无效 checksum、版本降级和未知 Formula 结构；相同输入可安全重跑。
6. Homebrew 在 Ubuntu runner 上执行 `style`、`audit --strict --online`、真实安装和 `brew test`。
7. 全部验证成功后才生成短期 GitHub App token；workflow 重新确认 tap revision 未变化，然后由 App bot 直接提交并推送 `homebrew-housegate/main`。

[`ci.yml`](../../../.github/workflows/ci.yml) 新增轻量 `release-tooling` job，在 PR 和 main push 上通过 Bazel target `//:homebrew_formula_updater_test` 执行 updater 的 Ruby 语法检查及 [`update_homebrew_formula_test.rb`](../../../.github/scripts/update_homebrew_formula_test.rb)。

workflow 级固定 concurrency group 防止两个 release 同时计算和推送版本；updater 的版本单调检查是第二层防护。GitHub 对 concurrency 的语义见 [Control workflow concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency)。

## 一次性 GitHub App 配置

GitHub 的内置 `GITHUB_TOKEN` 不能写另一个仓库，因此需要在 `housegate` organization 下创建专用 GitHub App：

1. Repository permissions 只授予 `Contents: Read and write`。
2. App 只安装到 `housegate/homebrew-housegate`。
3. 在 `housegate/housegate` Actions variables 中添加 `HOMEBREW_SYNC_APP_CLIENT_ID`。
4. 在 `housegate/housegate` Actions secrets 中添加 PEM 私钥 `HOMEBREW_SYNC_APP_PRIVATE_KEY`。
5. 如果 tap 的 `main` 有 ruleset/branch protection，需要允许该 App 直接 push；否则应将 `Publish formula` 改成 PR 模式，并额外授予 `Pull requests: write`。

workflow 用 `actions/create-github-app-token` 生成只覆盖 tap 的短期 installation token，并在 job 结束时撤销：[GITHUB_TOKEN scope](https://docs.github.com/en/actions/concepts/security/github_token)、[GitHub App in Actions](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/making-authenticated-api-requests-with-a-github-app-in-a-github-actions-workflow)、[`create-github-app-token`](https://github.com/actions/create-github-app-token)。

`Sync Homebrew` 也支持手动 `workflow_dispatch` backfill。合并并完成 GitHub App 配置后，可在 Actions 中运行 `Sync Homebrew`，输入 `v0.6.0`；workflow 会从该 exact release 的 `SHA256SUMS` 解析两平台 checksum，并走与正式 release 相同的验证和发布路径。

## 失败与恢复

- GitHub Release 未成功：`homebrew` 因 `needs` 不运行。
- Release 已成功、tap 更新失败：release 不回滚，tap 保持旧版本，整个 workflow 标红。**只点 “Re-run failed jobs” 重跑 Homebrew job**；不要重跑整个 release workflow，否则现有自动 bump 逻辑可能再切一个新 tag。
- tap main 在 checkout 后被修改：普通 `git push` 会 non-fast-forward 失败；禁止 force push。重跑 Homebrew job 会从新 main 开始，版本单调检查会阻止降级。
- 验收：`ruby -c Formula/housegate.rb`、`brew audit --strict --online housegate/housegate/housegate`、`brew install housegate/housegate/housegate`、`brew test housegate/housegate/housegate`。官方验证流程见 [Formula Cookbook](https://docs.brew.sh/Formula-Cookbook) 和 [Homebrew PR testing](https://docs.brew.sh/How-To-Open-a-Homebrew-Pull-Request)。

## 备选：tap-owned `repository_dispatch`

也可让 source workflow 只向 tap 发送 `{tag, darwin_sha, linux_sha}`，由 tap 自己的 workflow 用本仓库 `GITHUB_TOKEN` 更新 Formula。这能把更新逻辑留在 tap，但多一个 workflow，dispatch 本身仍需跨仓库凭据；`repository_dispatch` 是允许触发后续 workflow 的事件之一，见 [GITHUB_TOKEN event rules](https://docs.github.com/en/actions/concepts/security/github_token)。现阶段独立 `homebrew` job 更少组件、同步返回明确成功/失败，因此推荐先采用它。
