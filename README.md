#Streaming feature simulator

Streaming feature simulator is pretty self-explanatory, simulate the streaming features based on offline data. It provides the following features:

- Generate Feature Simulation SQL script
- Generate Feature Auditing SQL script

## Installation
1. Clone the repository from remote
``` 
git clone https://github.com/hao0L/Streaming_Feature_Simulator.git
   ```
2. Change the working directory to Streaming Feature Simulator
```
cd Streaming_Feature_Simulator
```
3. (optional) Activate Virtual Environment
```
source venv/bin/activate
```
4. Install dependencies required by Streaming Feature Simulator
```
pip install -r requirements.txt
```
5. Install package streaming_feature_simulator
```
python setup.py install --user
```
6. Testing
```
from streaming_feature_simulator.sql_generator import sql_generator
```



