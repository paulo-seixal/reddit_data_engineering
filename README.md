# reddit_data_engineering

A data pipeline to extract Reddit data from [r/portugal](https://www.reddit.com/r/portugal/).

Project was developed as a way to provide a good opportunity to develop skills and experience in a range of tools, such as Apache airflow, Docker, cloud based storage (AWS) adn PowerBI.

## Workflow

<img src="https://github.com/paulo-seixal/reddit_data_engineering/blob/main/images/workflow.png" width=70% height=70%>

1. Extract data using [Reddit API](https://www.reddit.com/dev/api/)
2. Create AWS resources with [Terraform](https://www.terraform.io)
3. Orchestrate with [Airflow](https://airflow.apache.org) in [Docker](https://www.docker.com)
4. Create [PowerBI](https://powerbi.microsoft.com/en-gb/) Dashboard 

## Output

<img src="https://github.com/paulo-seixal/reddit_data_engineering/blob/main/images/dashboard.png" width=70% height=70%>

* Final output from PowerBI.
