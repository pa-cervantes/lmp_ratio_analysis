from src.yes_energy.legacy_connector.yes_energy import ConnectorYESEnergy as YES_ENERGY

import pandas as pd

ISO = 'ERCOT'
node = 'HB_SOUTH'
datestart = '01/01/2017'
dateend = '09/30/2024'

NAME = YES_ENERGY(ISO, node, datestart, dateend)

# df_da = NAME.run('dalmp')
# df_rt = NAME.run(['dalmp', 'rtlmp'])




x=1