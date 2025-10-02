# Large Data Files

## 📦 CSV Data Files

The following CSV files are **not included** in the Git repository due to their large size:

- `cell_tower.csv` (~100+ MB)
- `support_tickets.csv`

## 📥 How to Obtain

These files are available via **Google Drive** and should be downloaded separately.

1. Download the files from the shared Google Drive folder
2. Place them in the `Setup/` directory (if needed for backup/restore)

## ⚠️ Note

These CSV files are **not required** for normal operation:
- The database already contains the production data
- The data generators create new data automatically
- These files were used for initial data load and are kept as backups

## 🔧 If You Need to Restore from CSV

If you need to restore data from these CSV backups:

1. Download the files from Google Drive
2. Place them in `Setup/` directory
3. Use Snowflake's `COPY INTO` command or the web interface to load them

Contact the data team if you need access to the Google Drive folder.

