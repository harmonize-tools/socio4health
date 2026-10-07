
from pathlib import Path

import pandas as pd


col_path = r"D:\EQUIPO\Documents HDD\Harmonize\GEIH\OUTPUT\GEIH_harmonized.csv"
per_path = r"D:\EQUIPO\Documents HDD\Harmonize\ENAHO\OUTPUT\ENAHO_harmonized.csv"
bra_pnad_dom_path = r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\OUTPUT\PNAD_harmonized_dom.csv"
bra_pnad_pes_path = r"D:\EQUIPO\Documents HDD\Harmonize\PNAD\OUTPUT\PNAD_harmonized_pes.csv"
bra_pnadc_path = r"D:\EQUIPO\Documents HDD\Harmonize\PNADC\OUTPUT\PNADC_harmonized.csv"
rd_path = r"D:\EQUIPO\Documents HDD\Harmonize\ENHOGAR\OUTPUT\ENHOGAR_harmonized.csv"


#col_fex = "FEX_C18_sum"
#per_fex = "FACTOR07_sum"
#bra_pnad_dom_fex = "V4611_sum"
bra_pnad_pes_fex = "V4729_sum"
bra_pnadc_fex = "V1032_sum"


def _clean_column_name(name):
	cleaned = str(name).strip().upper()
	cleaned = cleaned.replace("\ufeff", "")
	cleaned = cleaned.replace("Ï»¿", "")
	cleaned = cleaned.replace("ï»¿", "")
	return cleaned.strip()


def divide_columns_to_the_right(csv_path, fex_column):
	csv_path = Path(csv_path)
	df = pd.read_csv(csv_path)

	cleaned_columns = [_clean_column_name(col) for col in df.columns]
	cleaned_target = _clean_column_name(fex_column)

	if cleaned_target not in cleaned_columns:
		raise ValueError(f"Column '{fex_column}' was not found in {csv_path}")

	fex_index = cleaned_columns.index(cleaned_target)
	right_columns = list(df.columns[fex_index + 1 :])

	if not right_columns:
		print(f"No columns to the right of '{fex_column}' in {csv_path}")
		return

	factor = pd.to_numeric(df.iloc[:, fex_index], errors="coerce")

	for column in right_columns:
		numeric_values = pd.to_numeric(df[column], errors="coerce")
		df[column] = numeric_values.divide(factor)

	df.to_csv(csv_path, index=False)
	print(f"Updated {csv_path}")


if __name__ == "__main__":
	#divide_columns_to_the_right(col_path, col_fex)
	#divide_columns_to_the_right(per_path, per_fex)
	#divide_columns_to_the_right(bra_pnad_dom_path, bra_pnad_dom_fex)
	divide_columns_to_the_right(bra_pnad_pes_path, bra_pnad_pes_fex)
	divide_columns_to_the_right(bra_pnadc_path, bra_pnadc_fex)