import zipfile
from io import BytesIO

def zip_excels(file1: BytesIO, file2: BytesIO, name1="file1.xlsx", name2="file2.xlsx", report_text: str = None) -> BytesIO:
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        if file1 is not None:
            zip_file.writestr(name1, file1.getvalue())
        if file2 is not None:
            zip_file.writestr(name2, file2.getvalue())
        if report_text:
            zip_file.writestr("validation_report.txt", report_text)

    zip_buffer.seek(0)
    return zip_buffer