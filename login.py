from flask import Flask, request, jsonify, send_from_directory
import mysql.connector
import re
import bcrypt
import os

app = Flask(__name__)

# MySQL Database Connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="test"
)

# Path to store uploaded files
UPLOAD_FOLDER = os.path.normpath('uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Function to validate password
def validate_password(password):
    if len(password) <= 8:
        return "Password must be more than 8 characters."
    if not re.search(r'[A-Z]', password):
        return "Password must contain at least one uppercase letter."
    if not re.search(r'[a-z]', password):
        return "Password must contain at least one lowercase letter."
    if not re.search(r'[0-9]', password):
        return "Password must contain at least one numeric digit."
    if not re.search(r'[!@#$%^&*(),.?\":{}|<>]', password):
        return "Password must contain at least one special character."
    return None

# Function to check if a user exists
def find_user_by_email(email):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
    user = cursor.fetchone()
    cursor.close()
    return user

@app.route('/register', methods=['POST'])
def register():
    try:
        email = request.form.get('email')
        password = request.form.get('password')
        profile_image = request.files.get('profile_image')  # Optional file
        audio_file = request.files.get('audio_file')        # Optional file
        video_file = request.files.get('video_file')        # Optional file

        if not email:
            return jsonify({'error': 'Missing email'}), 400
        if not password:
            return jsonify({'error': 'Missing password'}), 400

        # Validate password
        validation_error = validate_password(password)
        if validation_error:
            return jsonify({'error': validation_error}), 400

        # Check if user exists
        user = find_user_by_email(email)
        if user:
            return jsonify({'error': 'User already exists'}), 400

        # Save files and construct paths
        profile_image_path = None
        audio_file_path = None
        video_file_path = None

        if profile_image:
            profile_image_path = os.path.join(UPLOAD_FOLDER, profile_image.filename)
            profile_image.save(profile_image_path)

        if audio_file:
            audio_file_path = os.path.join(UPLOAD_FOLDER, audio_file.filename)
            audio_file.save(audio_file_path)

        if video_file:
            video_file_path = os.path.join(UPLOAD_FOLDER, video_file.filename)
            video_file.save(video_file_path)

        # Hash password and save user
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        cursor = db.cursor()
        cursor.execute(
            "INSERT INTO users (email, password_hash, profile_image_path, audio_file_path, video_file_path) VALUES (%s, %s, %s, %s, %s)",
            (email, hashed_password, profile_image_path, audio_file_path, video_file_path)
        )
        db.commit()
        cursor.close()

        return jsonify({'message': f'Registration successful for {email}!'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/login', methods=['POST'])
def login():
    try:
        email = request.form.get('email')
        password = request.form.get('password')

        if not email:
            return jsonify({'error': 'Missing email'}), 400
        if not password:
            return jsonify({'error': 'Missing password'}), 400

        # Find the user by email
        user = find_user_by_email(email)
        if not user:
            return jsonify({'error': 'User not found!'}), 404

        # Verify the password
        if bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            # Construct the file URLs
            profile_image_url = None
            audio_file_url = None
            video_file_url = None

            if user['profile_image_path'] and os.path.exists(user['profile_image_path']):
                profile_image_url = f'{request.host_url}uploads/{os.path.basename(user["profile_image_path"])}'
            if user['audio_file_path'] and os.path.exists(user['audio_file_path']):
                audio_file_url = f'{request.host_url}uploads/{os.path.basename(user["audio_file_path"])}'
            if user['video_file_path'] and os.path.exists(user['video_file_path']):
                video_file_url = f'{request.host_url}uploads/{os.path.basename(user["video_file_path"])}'

            return jsonify({
                'message': f'Logged in successfully, Welcome {email}!',
                'profile_image_url': profile_image_url,
                'audio_file_url': audio_file_url,
                'video_file_url': video_file_url,
                'email': user['email']
            }), 200
        else:
            return jsonify({'error': 'Invalid login credentials'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users', methods=['GET'])
def get_users():
    try:
        email = request.args.get('email')  # Optional email query parameter

        cursor = db.cursor(dictionary=True)

        if email:
            # Fetch specific user by email
            cursor.execute("""
                SELECT id, email, profile_image_path, audio_file_path, video_file_path 
                FROM users WHERE email = %s
            """, (email,))
        else:
            # Fetch all users
            cursor.execute("""
                SELECT id, email, profile_image_path, audio_file_path, video_file_path 
                FROM users
            """)

        users = cursor.fetchall()
        cursor.close()

        # Format the response
        response = []
        for user in users:
            # Construct file URLs
            profile_image_url = None
            audio_file_url = None
            video_file_url = None

            if user['profile_image_path'] and os.path.exists(user['profile_image_path']):
                profile_image_url = f'{request.host_url}uploads/{os.path.basename(user["profile_image_path"])}'
            if user['audio_file_path'] and os.path.exists(user['audio_file_path']):
                audio_file_url = f'{request.host_url}uploads/{os.path.basename(user["audio_file_path"])}'
            if user['video_file_path'] and os.path.exists(user['video_file_path']):
                video_file_url = f'{request.host_url}uploads/{os.path.basename(user["video_file_path"])}'

            response.append({
                'id': user['id'],
                'email': user['email'],
                'profile_image_url': profile_image_url,
                'audio_file_url': audio_file_url,
                'video_file_url': video_file_url
            })

        return jsonify(response), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/uploads/<filename>')
def serve_file(filename):
    try:
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
        return send_from_directory(UPLOAD_FOLDER, filename, as_attachment=False)
    except Exception as e:
        return jsonify({'error': str(e)}), 404


if __name__ == "__main__":
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            profile_image_path VARCHAR(255) DEFAULT NULL,
            audio_file_path VARCHAR(255) DEFAULT NULL,
            video_file_path VARCHAR(255) DEFAULT NULL
        )
    """)
    db.commit()
    cursor.close()

    app.run(debug=True)
