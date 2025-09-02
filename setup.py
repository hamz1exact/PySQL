from setuptools import setup

APP = ['1.py']  # Replace with your actual Python file name
DATA_FILES = []
OPTIONS = {
    'argv_emulation': True,
    'packages': ['tkinter'],
    'iconfile': 'su_sql_icon.icns',  # Optional: add your icon file
    'plist': {
        'CFBundleName': 'Su-SQL',
        'CFBundleDisplayName': 'Su-SQL',
        'CFBundleIdentifier': 'com.yourname.su-sql',
        'CFBundleVersion': '1.0.0',
        'CFBundleShortVersionString': '1.0.0',
        'LSMinimumSystemVersion': '10.9.0',
    }
}

setup(
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)