from flask import Flask, request, jsonify, send_from_directory
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, create_refresh_token, get_jwt_identity
import mysql.connector
from flask_jwt_extended import get_jwt
import bcrypt
import os

app = Flask(__name__)

# JWT Configuration
app.config['JWT_SECRET_KEY'] = '857fcf106776c8bef1cb58f76779094f817f1a1b8d0b7399ef236ff006eb5138'  # Change this to a secure key
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = 600  # 1 hour expiration
jwt = JWTManager(app)

# MySQL Database Connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="test"
)

# File upload directory
UPLOAD_FOLDER = os.path.normpath('uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Helper function to find user by email
def find_user_by_email(email):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
    user = cursor.fetchone()
    cursor.close()
    return user

# Function to validate password strength
def validate_password(password):
    import re
    if len(password) < 8:
        return "Password must be at least 8 characters long."
    if not re.search(r'[A-Z]', password):
        return "Password must include at least one uppercase letter."
    if not re.search(r'[a-z]', password):
        return "Password must include at least one lowercase letter."
    if not re.search(r'[0-9]', password):
        return "Password must include at least one digit."
    if not re.search(r'[!@#$%^&*(),.?\":{}|<>]', password):
        return "Password must include at least one special character."
    return None

# User Registration Route
@app.route('/register', methods=['POST'])
def register():
    email = request.form.get('email')
    password = request.form.get('password')
    role = request.form.get('role', 'user')  # Default role: user

    # Handle file uploads
    profile_image = request.files.get('profile_image')
    audio_file = request.files.get('audio_file')
    video_file = request.files.get('video_file')

    if not email or not password:
        return jsonify({'error': 'Email and password are required'}), 400

    # Validate password strength
    password_error = validate_password(password)
    if password_error:
        return jsonify({'error': password_error}), 400

    # Check if the user already exists
    if find_user_by_email(email):
        return jsonify({'error': 'User already exists'}), 400

    # Save files to disk and get their paths
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

    # Hash the password
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    # Insert the user into the database
    try:
        cursor = db.cursor()
        cursor.execute(
            """
            INSERT INTO users (email, password_hash, role, profile_image_path, audio_file_path, video_file_path) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (email, hashed_password, role, profile_image_path, audio_file_path, video_file_path)
        )
        db.commit()
        cursor.close()
        return jsonify({'message': f'User {email} registered successfully!'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# User Login Route
@app.route('/login', methods=['POST'])
def login():
    email = request.form.get('email')
    password = request.form.get('password')

    if not email or not password:
        return jsonify({'error': 'Email and password are required'}), 400

    user = find_user_by_email(email)
    if not user:
        return jsonify({'error': 'User not found'}), 404

    # Verify the password
    if bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
        # Use the email as the identity (must be a string)
        access_token = create_access_token(
            identity=email,  # Identity should be a string
            additional_claims={'role': user['role']}  # Include additional claims
        )
        refresh_token = create_refresh_token(identity=email)

        return jsonify({
            'access_token': access_token,
            'refresh_token': refresh_token,
            'profile_image': user['profile_image_path'],
            'audio_file': user['audio_file_path'],
            'video_file': user['video_file_path']
        }), 200
    else:
        return jsonify({'error': 'Invalid credentials'}), 401

# Protected Route (Requires JWT)
@app.route('/protected', methods=['GET'])
@jwt_required()
def protected():
    current_user_email = get_jwt_identity()  # Identity is now the email
    role = get_jwt()["role"]  # Access additional claims
    return jsonify({
        'message': f'Welcome {current_user_email}!',
        'role': role
    }), 200

# File serving route
@app.route('/uploads/<filename>')
def serve_file(filename):
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    return send_from_directory(UPLOAD_FOLDER, filename)

# Initialize Database
def initialize_database():
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role VARCHAR(50) DEFAULT 'user',
            profile_image_path VARCHAR(255),
            audio_file_path VARCHAR(255),
            video_file_path VARCHAR(255)
        )
    """)
    db.commit()
    cursor.close()

# Run the App
if __name__ == '__main__':
    initialize_database()
    app.run(debug=True)
