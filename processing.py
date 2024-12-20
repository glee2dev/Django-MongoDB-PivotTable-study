import pandas as pd
import json
from operator import itemgetter

price_file = r"product_price_tag.csv"

# --- Function to Load JSON Files into DataFrames ---
def load_json_files(uploaded_files):
    dataframes = {}
    for file in uploaded_files:
        try:
            # Read the raw JSON content
            raw_json = json.load(file)

            # Process based on JSON structure
            if isinstance(raw_json, list):
                df = pd.DataFrame(raw_json)  # List -> DataFrame
            elif isinstance(raw_json, dict):
                # Dictionary -> Flattened DataFrame
                df = pd.json_normalize(raw_json)
            else:
                continue  # Skip unsupported JSON structure

            dataframes[file.name] = df  # Store DataFrame with filename as key

        except json.JSONDecodeError:
            continue  # Skip invalid JSON
        except Exception:
            continue  # Skip on other errors
    return dataframes

# --- Function to Load Price DataFrame ---
def load_price_data(file_path=price_file):
    """
    Load the price DataFrame from a specified CSV file.

    Parameters:
        - file_path: Path to the CSV file.

    Returns:
        - A pandas DataFrame containing price data.

    Raises:
        - FileNotFoundError: If the file does not exist.
        - RuntimeError: For other errors during file loading.
    """
    try:
        # Load the CSV with UTF-8-SIG encoding to handle special characters
        price_df = pd.read_csv(file_path, encoding='utf-8-sig')
        price_df['kind'] = price_df['kind'].astype('str')
        price_df['name'] = price_df['product_id']
        price_df['kind_name'] = price_df['kind']
        
        return price_df
    except FileNotFoundError:
        raise FileNotFoundError(f"Price CSV file '{file_path}' not found in the directory.")
    except Exception as e:
        raise RuntimeError(f"Error loading price data: {e}")

# --- Placeholder Processing Functions ---


def process_selected_files_1(dataframes, filenames, locations=['KOR']):
    """
    Process demographic data from JSON files.

    Parameters:
        - dataframes: List of DataFrames created from JSON files.
        - filenames: List of filenames corresponding to the dataframes.

    Returns:
        - A DataFrame containing the demographic data.
    """
    # Initialize a list to hold the results
    check = []

    # Process each DataFrame and corresponding filename
    for data, filename in zip(dataframes, filenames):
        # Extract source name from the filename (characters before the first `_`)
        name = filename.split('_')[0] + "_" + filename.split('_')[1]

        # Flatten the demographic data
        filtered_hist = [
            {
                'id': d['id'],
                'sex': '남' if d['gender'] == 1 else '여',
                'marriage': '기혼' if d['marriage'] > 0 else '미혼',
                'age': d['age'],
                'ages': d['ages'],
                'current_job': d['occupation_name'],
                'self_income': d['income'].get('self_income_range') or None,
                'hh_income': d['income'].get('hh_income_range') or None,
                'brand_name': d.get('brand_name', None),
                'last_ed': {
                    'high': '고등졸업',
                    'college': '학사졸업',
                    'master': '석사졸업',
                    'phd': '박사졸업'
                }.get(d['education']['history'][0]['level'], None),  # Map education level
                'region': d.get('region', 'KOR')  # Assuming region is part of d
            }
            # Convert DataFrame to list of records
            for d in data.to_dict(orient='records')
        ]

        # Create a DataFrame from the filtered demographic data
        hist_df = pd.DataFrame(filtered_hist)

        # Prepare the final output
        for _, row in hist_df.iterrows():
            # Check if the region is 'KOR' or None and include in the results
            if row['region'] in locations:
                check.append([
                    row['id'],
                    name,
                    row['sex'],
                    row['marriage'],
                    row['age'],
                    row['ages'],
                    row['current_job'],
                    row['self_income'],
                    row['hh_income'],
                    row['last_ed'],
                    row['region']
                ])

    # Sort the results
    check = sorted(check, key=itemgetter(0, 10))

    # Create the final DataFrame
    demo = pd.DataFrame(check, columns=[
        'id', 'source', 'sex', 'marriage', 'age', 'ages', 'current_job', 'self_income', 'hh_income', 'last_ed', 'region'
    ])

    return demo


def process_selected_files_2(dataframes, filenames, year=2010, locations=['KOR'], price_file=price_file):
    """
    Process selected JSON files to filter purchase history data and merge with price information.

    Parameters:
        - dataframes: List of DataFrames created from JSON files.
        - filenames: List of filenames corresponding to the dataframes.
        - year: The year to filter purchase history (default: 2010).
        - price_file: The path to the CSV file containing price information.

    Returns:
        - A DataFrame containing the final merged and processed results.
    """
    # Load price data
    price_df = load_price_data(price_file)

    # Check if price_df has required columns
    if not all(col in price_df.columns for col in ['name', 'kind_name']):
        raise KeyError(
            "The price file is missing required columns: 'name', 'kind_name'.")

    # Initialize an empty list to store the results
    check = []

    # Iterate over the provided DataFrames and filenames
    for df, filename in zip(dataframes, filenames):
        # Extract source name from the filename (characters after the first `_`)
        source_name = filename.split('_')[0] + "_" + filename.split('_')[1] if '_' in filename else filename
        filtered_df = df[df['region'].isin(locations)]

        # Flatten the purchase history and filter by the given year
        filtered_hist = []
        for d in filtered_df.to_dict(orient='records'):
            # Sort various histories by year or relevant keys
            sorted_purchase_history = sorted(
                d.get('purchase', {}).get('history', []), key=itemgetter('year'))
            sorted_education_history = sorted(
                d.get('education', {}).get('history', []), key=itemgetter('age'))
            sorted_res_history = sorted(d.get('residence', {}).get(
                'history', []), key=itemgetter('age_of_move_in'))
            sorted_int_history = sorted(d.get('interior', {}).get(
                'history', []), key=itemgetter('year'))
            sorted_child_history = sorted(d.get('children', {}).get(
                'history', []), key=itemgetter('year'))
            sorted_pet_history = sorted(d.get('pet', {}).get(
                'history', []), key=itemgetter('year')) if source_name != 'nielsen' else None
            sorted_car_history = sorted(d.get('vehicle', {}).get(
                'history', []), key=itemgetter('year'))

            # Process purchase history
            for hist in sorted_purchase_history:
                if hist['year'] >= year:
                    # Determine event type based on other histories
                    event = 'no'
                    for edhist in sorted_education_history:
                        if hist['year'] == edhist['year']:
                            event = 'edu'
                            break
                    for reshist in sorted_res_history:
                        if hist['year'] == reshist['year']:
                            event = 'res'
                            break
                    for inthist in sorted_int_history:
                        if hist['year'] == inthist['year']:
                            event = 'int'
                            break
                    for chhist in sorted_child_history:
                        if hist['year'] == chhist['year']:
                            event = 'child'
                            break
                    if sorted_pet_history:
                        for pethist in sorted_pet_history:
                            if hist['year'] == pethist['year']:
                                event = 'pet'
                                break
                    for carhist in sorted_car_history:
                        if hist['year'] == carhist['year']:
                            event = 'car'
                            break

                    filtered_hist.append({
                        'id': d['id'],
                        'age': d['age'],
                        'ages': d['ages'],
                        'year': hist['year'],
                        'name': hist['name'],
                        'kind_name': str(hist['kind_name']),
                        'brand_name': hist['brand_name'],
                        'birth': d['birth'],
                        'region': d.get('region', None),
                        'event': event
                    })

        # Create a DataFrame from the filtered history
        hist_df = pd.DataFrame(filtered_hist)

        # Skip if hist_df is empty
        if hist_df.empty:
            st.warning(
                f"Filtered purchase history is empty for source: {source_name}")
            continue

        # Merge the filtered history with the price information
        try:
            merged_df = hist_df.merge(
                price_df, on=['name', 'kind_name'], how='left')
        except KeyError as e:
            raise KeyError(
                f"Merge error: {e}. Check if 'name' and 'kind_name' columns exist in both DataFrames.")

        # Process the merged data
        for _, row in merged_df.iterrows():
            if row['region'] == 'KOR' or row['region'] is None:
                check.append([
                    'KOR',
                    source_name,
                    row['id'],
                    f"{source_name}_{row['id']}",  # CID
                    row['name'],
                    row['ages'],
                    # Calculate age at the purchase year
                    row['year'] - row['birth'],
                    row['year'],
                    row['kind_name'],
                    row['brand_name'],
                    row['price'],
                    row['event']
                ])
            elif row['region'] == 'US': 
                check.append([
                    'US',
                    source_name,
                    row['id'],
                    f"{source_name}_{row['id']}",  # CID
                    row['name'],
                    row['ages'],
                    # Calculate age at the purchase year
                    row['year'] - row['birth'],
                    row['year'],
                    row['kind_name'],
                    row['brand_name'],
                    row['price'],
                    row['event']
                ])

    # Sort the results
    check = sorted(check, key=itemgetter(0, 2, 6))

    # Create the final DataFrame
    result_df = pd.DataFrame(check, columns=[
        'country', 'source', 'id', 'cid', 'product', 'ages', 'age', 'year', 'detail', 'brand', 'price', 'event'
    ])

    return result_df



def process_selected_files_3(dataframes, filenames, locations=['KOR'], price_file=price_file):
    """
    Process selected JSON files to filter historical data by sorting of event sequence and merge with price information.

    Parameters:
        - dataframes: List of DataFrames created from JSON files.
        - filenames: List of filenames corresponding to the dataframes.
        - price_file: The path to the CSV file containing price information.

    Returns:
        - A DataFrame containing the final merged and processed results.
    """
    # Load price data
    price_df = load_price_data(price_file)

    # Ensure necessary columns are present in the price data
    required_cols = ['name', 'kind_name']
    if not all(col in price_df.columns for col in required_cols):
        raise KeyError("The price file is missing required columns: 'name', 'kind_name'.")

    # Initialize lists to collect results
    check = []

    # Create a dictionary for quick price lookup
    price_dict = {(row['product_id'], row['kind']): row['price'] for _, row in price_df.iterrows()}

    # Sample loop through dataframes
    for df, filename in zip(dataframes, filenames):
        source_name = filename.split('_')[0] if '_' in filename else filename
        
        filtered_df = df[df['region'].isin(locations)]

        # Convert DataFrame to list of dictionaries for faster processing
        records = filtered_df.to_dict(orient='records')

        for d in records:
        
            # Basic demographic info extraction
            id = d['id']
            region = d['region']
            marriage = '기혼' if d['marriage'] > 0 else '미혼'
            last_ed = {'high': '고등졸업', 'college': '학사졸업', 'master': '석사졸업', 'phd': '박사졸업'}[d['education']['history'][0]['level']]

            # Conditional check for marital status
            if marriage == '기혼':
                married_age = d['marriage'] - d['birth']
                check.append([source_name, region, id, 'marriage', 'marriage', '결혼', married_age, d['marriage']])

            # Processing educational history
            ed_hist = sorted(d['education']['history'], key=itemgetter('age'))
            check.extend([[source_name, region, id, 'edu', f'edu_{i+1}', last_ed, d['age'], hist['year']] for i, hist in enumerate(ed_hist)])

            # Processing job history
            job_hist = sorted(d['job']['history'], key=itemgetter('job_age_of'))
            check.extend([[source_name, region, id, 'job', f'job_{i+1}', hist['job_name'], hist['job_age_of'], hist['year'], hist['job_wage']] for i, hist in enumerate(job_hist)])

            # Processing residence history
            res_hist = sorted(d['residence']['history'], key=itemgetter('age_of_move_in'))
            check.extend([[source_name, region, id, 'move', f'move_{i+1}', f"{hist['ownership']}_{hist['type']}_{hist['size']}", hist['age_of_move_in'], hist['year']] for i, hist in enumerate(res_hist)])

            # Processing interior history
            int_hist = sorted(d['interior']['history'], key=itemgetter('year'))
            check.extend([[source_name, region, id, 'int', f'int_{i+1}', '인테리어', hist['year'] - d['birth'], hist['year'], hist['cost_amt']] for i, hist in enumerate(int_hist)])

            # Processing children history
            child_hist = d['children']['history']
            for i, hist in enumerate(child_hist):
                events = [
                    (hist['year'], f'자녀출산_{hist["order"]}', hist['age_of_birth']),
                    (hist.get('year_elementary'), f'자녀초입_{hist["order"]}', hist['year_elementary'] - d['birth'] if hist.get('year_elementary') is not None else None),
                    (hist.get('year_middle'), f'자녀중입_{hist["order"]}', hist['year_middle'] - d['birth'] if hist.get('year_middle') is not None else None),
                    (hist.get('year_high'), f'자녀고입_{hist["order"]}', hist['year_high'] - d['birth'] if hist.get('year_high') is not None else None),
                    (hist.get('married_year'), f'자녀결혼_{hist["order"]}', hist['married_year'] - d['birth'] if hist.get('married_year') is not None else None)
                ]
                check.extend([[source_name, region, id, 'child', f'child_{i+1}', event[1], event[2], event[0]] for event in events if event[0] is not None and event[0] > 0])

            # Processing pet history
            pet_hist = sorted(d['pet']['history'], key=itemgetter('year'))
            check.extend([[source_name, region, id, 'pet', f'pet_{i+1}', '애완동물입양', hist['year'] - d['birth'], hist['year'], hist['kind']] for i, hist in enumerate(pet_hist)])

            # Processing car history
            car_hist = sorted(d['vehicle']['history'], key=itemgetter('year'))
            check.extend([[source_name, region, id, 'car', f'car_{i+1}', '자동차구매', hist['year'] - d['birth'], hist['year'], f"{hist['purchase']}_{hist['make']}_{hist['kind']}"] for i, hist in enumerate(car_hist)])

            # Processing purchase history
            purch_hist = sorted(d['purchase']['history'], key=itemgetter('year'))
            for i, hist in enumerate(purch_hist):
                price_info = price_dict.get((hist['name'], hist['kind_name']), None)
                if price_info:
                    check.append([source_name, region, id, 'purch', f'purch_{i+1}', f"구매:{hist['name']}", hist['year'] - d['birth'], hist['year'], hist['kind_name'], hist['brand_name'], price_info])

    # Sorting the collected data
    check = sorted(check, key=itemgetter(1, 2, 6))

    # Creating a DataFrame for the results
    listup = pd.DataFrame(check, columns=['source', 'region', 'id', 'header', 'sequence', 'condition', 'age', 'year', 'detail', 'brand', 'price'])

    return listup

def process_selected_files_4(dataframes):
    return pd.concat(dataframes, axis=0, ignore_index=True)
