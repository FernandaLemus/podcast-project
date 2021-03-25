

### Why did you choose the model you chose?

I chose the data model so that the average of the ratings was available to the analysts without the need for extra processing, 
as it can be seen that there are too many reviews.

Regarding the ETL, I chose Airflow because it seems to me that it offers a good execution time versus pandas for exmple and 
has the aditional advantage of their graphic interface and it can be shared with tha analytics teams so they can understand what is happening. 
The copy command is for its efficiency against inserts.


### Propose how often the data should be updated and why.

This process can be modify to run once a week and just add the extra reviews and adjust the averages.

### Post your write-up and final data model in a GitHub repo.

¡This is the repo :D!


### Include a description of how you would approach the problem differently under the following scenarios:
  
  1. If the data was increased by 100x: I would include pyspark in the the Airflow operators.
  2. If the pipelines were run on a daily basis by 7am: I would change all the pipeline to a lambda in AWS that can be triggered at any time. I can also put the trigger on the arrival of a file to S3
  3. If the database needed to be accessed by 100+ people: I would create a real-time copy of the database and would connect it to a data visualization tool, such as metabase, periscope or tableu. In this way, you would not saturate the connections and queries to the productive database.
