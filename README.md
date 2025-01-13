# nature
Get room temperature and humidity from Nature API, then insert into Autonomous DB.

# func.py Usage 

* Get Nature Cloud API key and set to `configs.apikey`.
* Download OCI Autonomous Database wallet from OCI Console.
  * Plase tnsnames.ora and ewallet.pem to `/function/db/` .
  * You can specify anothrer location by  `configs.db_config_dir`.

