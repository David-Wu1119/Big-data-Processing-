# Publish This As Your Own Public Repository

The assignment requires a repository that you own, not your teammate.

## Recommended steps

1. Create a new public GitHub repository under your own account.
2. Name it something explicit, for example:
   - `nyc-taxi-weather-demand-forecast`
3. From this `public_repo/` directory, run:

```bash
cd /Users/yanzewu/Desktop/CS_BigData_472/Hws/MAY3_PUBLIC_ARTIFACTS/public_repo
git init
git add .
git commit -m "Initial public project release"
git branch -M main
git remote add origin <YOUR_PUBLIC_GITHUB_REPO_URL>
git push -u origin main
```

## Before pushing

- Verify the repository owner is your account.
- Verify the repository is public.
- Verify `README.md` renders correctly on GitHub.
- Verify the repository contains code and screenshots.
- Do not publish secrets, tokens, or private credentials.

## What URL to submit

Submit the public GitHub repository URL, not a local path.
