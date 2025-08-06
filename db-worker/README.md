# Dictionary Manager Web Interface

A simple web interface to manage dictionary entries for the LoginputEngine project.

## Setup

1. Ensure you have Python 3.7+ installed
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Running the Application

1. Start the Flask development server:
   ```
   python app.py
   ```
2. Open your web browser and navigate to:
   ```
   http://localhost:5001
   ```

## Features

- View the current number of dictionary entries
- Add new dictionary entries with word, pinyin, and optional weight
- Automatically process the dictionary by running `main.py` after each addition
- View the output of the processing step
- Responsive design that works on both desktop and mobile devices

## File Structure

- `app.py` - Main Flask application
- `templates/` - HTML templates
  - `base.html` - Main template with the web interface
- `requirements.txt` - Python dependencies
