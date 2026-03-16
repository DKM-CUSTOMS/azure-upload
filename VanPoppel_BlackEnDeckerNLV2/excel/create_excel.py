from io import BytesIO
import openpyxl
import logging
from sofidelV2.utils.number_handlers import safe_float_conversion

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def write_to_excel(json_string, second_layout=False):
    try:
        logger.info("Starting Excel file creation process")
        
        # Create workbook
        logger.info("Creating new workbook")
        wb = openpyxl.Workbook()
        ws = wb.active 

        data = json_string
        logger.debug(f"Input data: {data}")

        # Header definition
        header1 = [
            "VAT exporter", "Contact", "Commericial reference", "Other ref", "Freight",
            "Goods location", "Export office", "Exit office", "Name", "Street + number",
            "Postcode", "city", "Country", "Inco Term", "Place", "Container", "Truck", "Rex/Other", "ILS NUMBER", "EUR1" if second_layout else ""
        ]
        logger.debug("Header1 defined")

        # Address processing
        try:
            logger.info("Processing address information")
            address = data.get('PlaceOfDelivery', {})
            logger.debug(f"Address data: {address}")
            
            name = address.get('company_name', '')
            street = address.get('street', '')
            city = address.get('city', '')
            code_postal = address.get('postal_code', '')
            country = address.get('country_code', '')
            logger.debug("Address components extracted")
        except Exception as e:
            logger.error(f"Error processing address: {str(e)}")
            name = street = city = code_postal = country = ''

        # Incoterm processing
        try:
            logger.info("Processing Incoterm")
            Incoterm = data.get('Incoterm', '')
            term, place = "", ""
            
            if Incoterm:
                if isinstance(Incoterm, str):
                    parts = Incoterm.split(' ', 1)
                    term = parts[0]
                    place = parts[1] if len(parts) > 1 else ""
                elif isinstance(Incoterm, (list, tuple)):
                    term = Incoterm[0] if len(Incoterm) > 0 else ""
                    place = Incoterm[1] if len(Incoterm) > 1 else ""
            
            logger.debug(f"Incoterm processed - term: {term}, place: {place}")
        except Exception as e:
            logger.error(f"Error processing Incoterm: {str(e)}")
            term = place = ""

        # Freight value processing
        try:
            logger.info("Processing FreightCost")
            freight_data = data.get('FreightCost', {})
            FreightValue = safe_float_conversion(freight_data.get('value', 0))
            logger.debug(f"FreightValue: {FreightValue}")
        except Exception as e:
            logger.error(f"Error processing FreightCost: {str(e)}")
            FreightValue = 0

        # First set of values
        try:
            logger.info("Preparing first set of values")
            values1 = [
                data.get('Vat Number', ''),
                data.get('Contact', ''),
                data.get('ShipmentReference', ''),
                data.get('Other Ref', ''),
                FreightValue,
                data.get("Location", ''),
                data.get('Exit Port BE', ''),
                data.get('OfficeOfExit', ''),
                name,
                street,
                code_postal,
                city,
                country,
                term,
                place,
                data.get('container', ''),
                data.get('Wagon', ''),
                data.get("Customs code", ''),
                data.get("ILS_NUMBER", ''),
                "N954" if second_layout else ""
            ]
            logger.debug("Values1 prepared")
        except Exception as e:
            logger.error(f"Error preparing values1: {str(e)}")
            values1 = [''] * len(header1)

        # Second header and items processing
        header2 = [
            "Commodity", "Description", "Article", "Collis", "Gross", "Net",
            "Origin", "Invoice value", "Currency", "Statistical Value",
            "Pieces", "Invoice number", "Invoice date", "Rex/other"
        ]
        logger.debug("Header2 defined")

        rows_data = []
        try:
            logger.info("Processing items data")
            if "Items" in data:
                for obj in data["Items"]:
                    try:
                        mini_row = []
                        for ordered_key in header2:
                            try:
                                if ordered_key == "Commodity":
                                    mini_row.append(obj.get("HSCode", ''))
                                elif ordered_key == "Pieces":
                                    mini_row.append(obj.get("Quantity", ''))
                                elif ordered_key == "Gross":
                                    mini_row.append(obj.get("gross_weight_kg", ''))
                                elif ordered_key == "Net":
                                    mini_row.append(obj.get("NetWeight", ''))
                                elif ordered_key == "Invoice value":
                                    mini_row.append(obj.get("Amount", "")) 
                                elif ordered_key == "Invoice number":
                                    mini_row.append(obj.get("InvoiceNumber", ''))
                                elif ordered_key == "Invoice date":
                                    mini_row.append(obj.get("InvoiceDate", ''))
                                elif ordered_key == "Rex/other":
                                    mini_row.append(obj.get("EUR1_Flag", ''))
                                else:    
                                    mini_row.append(obj.get(ordered_key, ''))
                            except Exception as e:
                                logger.error(f"Error processing item field {ordered_key}: {str(e)}")
                                mini_row.append('')
                        rows_data.append(mini_row)
                    except Exception as e:
                        logger.error(f"Error processing item row: {str(e)}")
                        continue

            # --- Sorting rows by HS code (first column of header2 = "Commodity") ---
            def hs_key(row):
                hs = str(row[0]).strip()
                if not hs:
                    return float("inf")  # empty hs_code -> bottom
                try:
                    return int(hs.replace(".", "").replace(" ", ""))
                except:
                    return float("inf")  # non-numeric hs_code -> bottom

            rows_data.sort(key=hs_key)

            logger.info(f"Processed {len(rows_data)} item rows (sorted by HS code)")
        except Exception as e:
            logger.error(f"Error processing items: {str(e)}")

        # Writing to worksheet
        try:
            logger.info("Writing data to worksheet")
            
            # Write headers and values
            ws.append(header1)
            ws.append(values1)
            
            # Write empty rows
            ws.append([""] * len(header1))
            ws.append([""] * len(header1))
            
            # Write totals
            row5 = ["Total invoices", "", "EORI nr carrier", "Route"]
            row5.extend([""] * max(0, len(header1) - len(row5)))
            ws.append(row5)
            
            total = safe_float_conversion(data.get('Total Value', 0))
            eori_val = data.get('EORI', '')
            route_val = data.get('Route', '')
            row6 = [total, "", eori_val, route_val]
            row6.extend([""] * max(0, len(header1) - len(row6)))
            ws.append(row6)
            ws.append([""] * len(header1))
            
            ws.append(["Total Collis"])
            total_pallets = safe_float_conversion(data.get('Collis', 0))
            ws.append([total_pallets])
            ws.append([""] * len(header1))
            
            ws.append(["Total Gross", "Total Net"])
            total_net = safe_float_conversion(data.get('NetWeight', 0))
            total_weight = safe_float_conversion(data.get('GrossWeight', 0))
            ws.append([total_weight, total_net])
            ws.append([""] * len(header1))
            
            # Write items
            ws.append(["Items"])
            ws.append(header2)
            for arr in rows_data:
                ws.append(arr)
            
            logger.info("Data written to worksheet")
        except Exception as e:
            logger.error(f"Error writing to worksheet: {str(e)}")
            raise

        # Adjust column widths
        try:
            logger.info("Adjusting column widths")
            for col in ws.columns:
                max_length = 0
                column = col[0].column_letter
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                ws.column_dimensions[column].width = adjusted_width
            logger.info("Column widths adjusted")
        except Exception as e:
            logger.error(f"Error adjusting column widths: {str(e)}")

        # Save to BytesIO
        try:
            logger.info("Saving workbook to BytesIO")
            file_stream = BytesIO()
            wb.save(file_stream)
            file_stream.seek(0)
            logger.info("Workbook saved successfully")
            return file_stream
        except Exception as e:
            logger.error(f"Error saving workbook: {str(e)}")
            raise

    except Exception as e:
        logger.error(f"Fatal error in write_to_excel: {str(e)}")
        raise
