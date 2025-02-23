import fitz  # PyMuPDF
import os
import psycopg2

#Create a connection with database
try:
    conn = psycopg2.connect(
        database = 'resume_db',
        user = 'postgres',
        password = 'Password',
        host = 'localhost',
        port = 5432

    )
    print('Database connected....')
except Exception as e:
    print(e)

cur = conn.cursor()


cur.execute('''Create table if not exists raw_resume_text(
            pdf_number INT PRIMARY KEY,
            raw_text TEXT)''')

print('Table created successfully...')

#Extract data from each pdf
def extract_text_from_pdf(pdf_path):
    """Extracts text from a single PDF file."""
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text("text")  # Extract text from each page
    return text.strip()

#Extract data from each folder
def process_pdfs(pdf_folder):
    """Recursively processes all PDFs in a folder (including subfolders) and extracts text."""
    for root, dirs, files in os.walk(pdf_folder):  # os.walk recursively traverses directories
        for pdf_file in files:
            if pdf_file.endswith(".pdf"):
                pdf_path = os.path.join(root, pdf_file)
                extracted_text = extract_text_from_pdf(pdf_path)
                pdf_number = os.path.splitext(pdf_file)[0]
                if(pdf_number==10554236):
                    print(extracted_text)
                
                cur.execute(
                    "INSERT INTO raw_resume_text (pdf_number,raw_text) VALUES (%s,%s)",
                    (pdf_number,extracted_text,)
                )
                


def main():
    pdf_folder = "data/" 
    process_pdfs(pdf_folder)

if __name__ == "__main__":
    main()


conn.commit()
cur.close()
conn.close()