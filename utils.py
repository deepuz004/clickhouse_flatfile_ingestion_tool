import os

def allowed_file(filename, allowed_extensions=None):
    """
    Check if the uploaded file has an allowed extension.
    """
    if allowed_extensions is None:
        allowed_extensions = {'csv'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

def save_uploaded_file(upload, upload_folder):
    """
    Save an uploaded file to a given directory.
    """
    if not os.path.exists(upload_folder):
        os.makedirs(upload_folder)
    filepath = os.path.join(upload_folder, upload.filename)
    upload.save(filepath)
    return filepath
