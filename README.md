# Daily Weather ETL 

<h3>Goal</h3>

<p>In order to expand data engineering skills, the goal for this project was to deploy an airflow instance via Docker containers that will run a daily DAG to collect weather data utilizing the Open Weather API. The criteria to be met included:</p>
<ul>
    <li>Build Docker images and deploy containers via Docker Desktop</li>
    <li>Create a dag that utlized multiple operator types (Python, Postgres)</li>
    <li>Utilize API calls for data retrieval (with possible multi-threaded or multi-processing added later)</li>
    <li>Utilize the following libraries: Pandas, Boto3, SQLAlchemy</li>
    <li>Levarage AWS s3 bucket for daily file storage and archiving</li>
</ul>

<h3>Process</h3>

<p>Building out this project started with the standard docker-compose.yml file as available from Airflow. By deploying the file initially, and running the <i>init</i> process, the basic airflow structure was created within the repository and confirmed that the containers as prescribed worked correctly.</p>

Airflow Docker Setup can be found <a href="https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html"><i>here</i></a>.

<p>The next step was to build out the API calls and validate the Extract and Transform process. This was initially done within a Jupyter Notebook to work through the basic steps. This code was converted into .py files for use by the DAG python operator. Finally, the DAG itself was created, leveraging Python and Postgres operators, and Postgres hooks.</p>
