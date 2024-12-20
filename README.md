# MLOps with Databricks
This is the repository for the LinkedIn Learning course `MLOps with Databricks`. The full course is available from [LinkedIn Learning][lil-course-url].

![lil-thumbnail-url]

In this course, MLOps expert Maria Vechtomova introduces the components and principles that you must understand to successfully deploy machine learning models to production on Databricks. Dive into the step-by-step process of using Feature Engineering in Unity Catalog, tracking model experiments in mlflow, registering a model in Unity Catalog, and deploying your model using Databricks model serving. Explore the use cases where Feature Serving can be used and find out how to deploy a Feature Serving endpoint. Plus, learn how to package your code, deploy your project using Databricks Asset Bundles, and monitor your ML application using inference tables and Lakehouse monitoring.

Learning objectives
* Explain main components and principles required to deploy machine learning models to production on Databricks.
* Identify how to use experiment tracking system, model registry, feature engineering, model/feature serving, and other features required to deploy ML applications.
* Articulate how to package your Python code using best practices and deploy your project using Databricks Asset Bundles.
* Review how to monitor your ML applications.

_See the readme file in the main branch for updated instructions and information._
# Instructions
This repository has branches for each of the videos in the course. You can use the branch pop up menu in github to switch to a specific branch and take a look at the course at that stage, or you can add `/tree/BRANCH_NAME` to the URL to go to the branch you want to access.

## Branches
The branches are structured to correspond to the videos in the course. The naming convention is `CHAPTER#_MOVIE#`. As an example, the branch named `02_03` corresponds to the second chapter and the third video in that chapter. 
Some branches will have a beginning and an end state. These are marked with the letters `b` for "beginning" and `e` for "end". The `b` branch contains the code as it is at the beginning of the movie. The `e` branch contains the code as it is at the end of the movie. The `main` branch holds the final state of the code when in the course.

When switching from one exercise files branch to the next after making changes to the files, you may get a message like this:

    error: Your local changes to the following files would be overwritten by checkout:        [files]
    Please commit your changes or stash them before you switch branches.
    Aborting

To resolve this issue:
	
    Add changes to git using this command: git add .
	Commit changes using this command: git commit -m "some message"

## Installing
1. Clone this repository into your local machine using the terminal (Mac), CMD (Windows), or a GUI tool like SourceTree.
2. Create a Databricks cluster with runtime 15.4 LTS.
3. Update databricks.yml file. Make sure databricks.yml contains a valid host.
4. Install Databricks VS code extension: https://docs.databricks.com/en/dev-tools/vscode-ext/install.html
5. Configure VS code extension: https://docs.databricks.com/en/dev-tools/vscode-ext/configure.html
6. If you want to use uv and do not have it, install uv: https://docs.astral.sh/uv/getting-started/installation/
7. To use these exercise files, set up the environment:
```
    uv venv -p 3.11 .venv
    source .venv/bin/activate
    uv pip install -r pyproject.toml --all-extras
    uv lock
```

## Instructor

Maria Vechtomova

MLOps Tech Lead | Databricks MVP | Public Speaker

[0]: # (Replace these placeholder URLs with actual course URLs)

[lil-course-url]: https://www.linkedin.com/learning/mlops-with-databricks
[lil-thumbnail-url]: https://media.licdn.com/dms/image/v2/D4D0DAQG9Mx2RIXHi3A/learning-public-crop_675_1200/learning-public-crop_675_1200/0/1733879753628?e=2147483647&v=beta&t=WgQPn2ujKbrHAftrG5IDDC0348ZdJmRvCNx8cpK-5ZI

