from setuptools import find_packages, setup

install_requires = [
    "torch",                    # 1.10.0
    "opencv-python==4.5.2.52",  # 4.5.2.52
    "gdown==4.4.0",             # 4.4.0
    ### data ###
    "awscli",
    "pytz==2021.1",             # time zone, 2021.1
    "ephem==3.7.7.1",           # 3.7.7.1
    "geopy==2.1.0",             # 2.1.0
    "geotiff",                  # for tiff data, generally not used
    "azure-storage-blob==12.16.0", #12.16.0
    ### system ###
    'wsrlib @ git+https://github.com/darkecology/pywsrlib#egg=wsrlib', # 6ba705d
    'detectron2 @ git+https://github.com/facebookresearch/detectron2.git#egg=detectron2', # 0.6
    'imageio==2.9.0',           # saving gif, 2.9.0
    'scikit-learn==0.24.2',     # nearest neighbor search using ball tree for wind farm, 0.24.2
]

setup(
    name="roosts",
    version="0.1.0",
    description="The Pytorch repo implements a machine learning system for detecting and tracking "
                "communal bird roosts in weather surveillance radar data. ",
    packages=find_packages(),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.8.0",
    install_requires=install_requires,
    classifiers=[
        'Development Status :: 3 - Alpha',
    ]
)
