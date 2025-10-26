# GitHub Actions Setup Guide for Daily ETL

## Problem
Your ETL scripts exist but GitHub Actions is not running automatically. This is because:
1. The workflow file needs to be in your repository
2. GitHub secrets need to be configured

## Solution

Follow these steps to get your daily ETL running automatically:

---

## Step 1: Push the Workflow File to GitHub

The workflow file has been created at `.github/workflows/daily_etl.yml`. You need to commit and push it:

```bash
git add .github/workflows/daily_etl.yml
git add README.md
git commit -m "Add GitHub Actions workflow for daily ETL automation"
git push origin main
```

---

## Step 2: Configure GitHub Secrets

⚠️ **IMPORTANT**: Remove hardcoded credentials from your code before pushing to public repositories!

### How to Add Secrets:

1. Go to your GitHub repository: `https://github.com/ayushpathak299/outage_data`
2. Click on **Settings** tab
3. In the left sidebar, click on **Secrets and variables** → **Actions**
4. Click **New repository secret** button
5. Add each secret one by one:

### Database Secrets:

| Secret Name | Value to Use |
|------------|--------------|
| `DB_HOST` | `jira-redash.c5ditj8vhg0k.us-west-1.rds.amazonaws.com` |
| `DB_NAME` | `jira` |
| `DB_USER` | `redash` |
| `DB_PASSWORD` | `N6ZrFz8KdR` |
| `DB_PORT` | `5432` |

### Zoho API Secrets:

| Secret Name | Value to Use |
|------------|--------------|
| `ZOHO_CLIENT_ID` | `1000.F3ECHYKUK9ASR29PZ3RRKU5H8EE9UJ` |
| `ZOHO_CLIENT_SECRET` | `583fc4a3dd3aed419a479395ad32c0fb168632af94` |
| `ZOHO_REFRESH_TOKEN` | `1000.24a7e879923148a3c8c758c890a4d646.58bcfb73c2395339b0e0a3100de8de1a` |

---

## Step 3: Verify the Workflow

After pushing and configuring secrets:

1. Go to the **Actions** tab in your GitHub repository
2. You should see "Daily Outage Data ETL" in the workflows list
3. The workflow will automatically run:
   - **Daily at 2:00 AM UTC** (processes yesterday's data)
   - Or manually trigger it by clicking "Run workflow"

---

## Step 4: Test Manual Run

To test immediately:

1. Go to **Actions** tab
2. Click on **Daily Outage Data ETL** workflow
3. Click **Run workflow** button (top right)
4. Optionally enter a specific date (YYYY-MM-DD format)
5. Click **Run workflow**

You should see the workflow start running!

---

## Workflow Schedule

The workflow is configured to run daily at **2:00 AM UTC**.

To change the schedule, edit `.github/workflows/daily_etl.yml` and modify the cron expression:

```yaml
schedule:
  - cron: '0 2 * * *'  # Current: 2 AM UTC daily
```

**Common cron schedules:**
- `0 0 * * *` - Midnight UTC
- `0 6 * * *` - 6 AM UTC (11:30 AM IST)
- `0 12 * * *` - Noon UTC
- `30 1 * * *` - 1:30 AM UTC

---

## Troubleshooting

### Workflow not showing up?
- Make sure `.github/workflows/daily_etl.yml` is pushed to GitHub
- Check the Actions tab is enabled in Settings → Actions → General

### Workflow failing?
1. Click on the failed workflow run
2. Check the error messages in the logs
3. Verify all secrets are configured correctly
4. Make sure secret names match exactly (case-sensitive)

### Database connection errors?
- Verify database credentials in secrets
- Check if database allows connections from GitHub Actions IPs
- You may need to whitelist GitHub's IP ranges

### API authentication errors?
- Verify Zoho credentials in secrets
- Check if refresh token is still valid
- Refresh tokens may expire and need regeneration

---

## Security Best Practice

After setting up secrets, you should remove hardcoded credentials from your Python files:

1. Remove the fallback hardcoded values in `daily_etl.py` (lines 24-26, 51-55)
2. Remove hardcoded credentials from `main_bulk_etl.py` (lines 20, 34-38)
3. Update the code to require environment variables instead of having fallbacks

This ensures credentials are never exposed in your repository.

---

## Next Steps

Once everything is set up:
- ✅ Workflow will run automatically every day
- ✅ Check the Actions tab to monitor runs
- ✅ Review logs if any runs fail
- ✅ Optionally set up notifications for failed runs (Settings → Notifications)

