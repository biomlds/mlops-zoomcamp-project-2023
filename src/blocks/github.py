from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/biomlds/mlops-zoomcamp-project-2023",
    # access_token=<my_access_token> # only required for private repos
)
block.get_directory("blocks") # specify a subfolder of repo
block.save("gh")