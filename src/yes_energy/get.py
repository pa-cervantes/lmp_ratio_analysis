from src.yes_energy.legacy_connector.support.lmp_yes import LMP_YES as LMP

src = 'LV1A_LV1B'
snk = 'HB_SOUTH'

file_path = "C:/Users/DENJC4/OneDrive - PA Consulting Group/Documents/programming/tbn/1. test/"  # this folder needs to exist on your local computer for the code to run and save out.
file_name = 'test_yes'  # this will overwrite any existing files with the same name. Also, the code will stop if you have an existing file with the same name open
iso = "ERCOT"  # "NEISO" # "PJM"  # market = "ERCOT" "PJM" "NYISO" "MISO" "SPP"
nodes = [(src, snk)]
startdate = '01/01/2017'
enddate = '09/30/2024'


lmp_ratio = LMP(iso=iso,
                nodes=nodes,
                startdate=startdate,
                enddate=enddate,
                file_path=file_path,
                file_name=file_name).pull_data()

y=1
