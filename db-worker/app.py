from flask import Flask, render_template, request, jsonify, Response, stream_with_context
import subprocess
import os
import json
import time
import sys
from pathlib import Path
from threading import Lock
from queue import Queue, Empty

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this in production

# Paths
BASE_DIR = Path(__file__).parent.parent
WORD_FILE = os.path.join(BASE_DIR, 'res', 'word.txt')
MAIN_SCRIPT = os.path.join(BASE_DIR, 'main.py')

def get_word_count():
    try:
        with open(WORD_FILE, 'r', encoding='utf-8') as f:
            return len([line for line in f if line.strip()])
    except FileNotFoundError:
        return 0

def add_words(entries):
    # Read existing words
    try:
        with open(WORD_FILE, 'r', encoding='utf-8') as f:
            existing_lines = set(f.readlines())
    except FileNotFoundError:
        existing_lines = set()

    new_lines_to_add = []
    duplicates_found = []
    
    # Validate, format and check for duplicates
    for entry in entries:
        word = entry.get('word', '').strip()
        pinyin = entry.get('pinyin', '').strip()
        
        if not word or not pinyin:
            raise ValueError("Both word and pinyin are required for each entry")
            
        formatted_line = f"{word}\t{pinyin}\n"
        
        if formatted_line in existing_lines or formatted_line in new_lines_to_add:
            duplicates_found.append(entry)
        else:
            new_lines_to_add.append(formatted_line)
    
    # Append all new entries to the file if any
    if new_lines_to_add:
        with open(WORD_FILE, 'a', encoding='utf-8') as f:
            f.writelines(new_lines_to_add)

    return len(new_lines_to_add), duplicates_found

@app.route('/')
def index():
    word_count = get_word_count()
    return render_template('index.html', word_count=word_count)

def stream_process_output(process, queue):
    for line in process.stdout:
        queue.put(line)
    process.wait()
    queue.put(None)  # Signal that the process is done

@app.route('/add-words', methods=['POST'])
def add_words_route():
    try:
        data = request.get_json()
        if not data or not isinstance(data, list):
            return jsonify({
                'success': False,
                'error': 'Invalid data format. Expected an array of {word, pinyin} objects.'
            }), 400
            
        # Add all words
        added_count, duplicates = add_words(data)
        
        if duplicates:
            return jsonify({
                'success': False,
                'error': 'Duplicate entries found',
                'duplicates': duplicates
            }), 400
        
        return jsonify({
            'success': True,
            'message': f'Successfully added {added_count} words',
            'count': added_count
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/process')
def process_route():
    def generate():
        # Start the process safely and stream output as SSE
        try:
            process = subprocess.Popen(
                [sys.executable, MAIN_SCRIPT],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                cwd=BASE_DIR,
                env={**os.environ, 'PYTHONUNBUFFERED': '1'}
            )
        except FileNotFoundError as e:
            msg = f"Failed to start process: {e}"
            yield f"data: {json.dumps({'status': 'error', 'message': msg})}\n\n"
            return
        except Exception as e:
            msg = f"Unexpected error starting process: {e}"
            yield f"data: {json.dumps({'status': 'error', 'message': msg})}\n\n"
            return

        try:
            # Stream the output
            for line in iter(process.stdout.readline, ''):
                yield f"data: {json.dumps({'output': line})}\n\n"
            
            process.wait()

            # Finally, send the completion status
            return_code = process.returncode
            if return_code == 0:
                yield f"data: {json.dumps({'status': 'completed', 'message': 'Process completed successfully!'})}\n\n"
            else:
                yield f"data: {json.dumps({'status': 'error', 'message': f'Process failed with return code {return_code}'})}\n\n"
        finally:
            if process.stdout:
                process.stdout.close()
    
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'  # Disable buffering in nginx if used
        }
    )

if __name__ == '__main__':
    # Create the word.txt file if it doesn't exist
    os.makedirs(os.path.dirname(WORD_FILE), exist_ok=True)
    if not os.path.exists(WORD_FILE):
        open(WORD_FILE, 'w').close()
        
    app.run(debug=True, port=5001)
