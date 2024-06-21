#RemoteNestDetection

R script developed to detect nesting attempts using GPS and ODBA data.

1. This repository presents a script that is associated with a paper that was recently submitted for publication. We will provide a reference to the paper explaining the script after publication.

2. Due to the deprecation of the maptools package in October 2023, this script uses the R package suntools (https://cran.r-project.org/web/packages/suntools/index.html) to estimate sunrise and sunset times (function sunriset).

3. We provide test files for both male and female sandgrouses, along with an example of the dictionary that adds information for each individual. We also include a folder with the expected results for each step of the script.

4. Since sandgrouses are sensitive to disturbances and the original coordinates were associated with nest sites, we intentionally modified the coordinates in the test files.

5. Since both individuals in the examples were tagged with Druid devices, testing the script should take a distance threshold of 31 meters into account.

6. For any doubts about the script please contact goncaloferraz@cibio.up.pt

   

