#CEPSim 
CEPSim is simulator for cloud-based CEP and SP systems.  CEPSim extends [CloudSim](http://www.cloudbus.org/cloudsim/) using a query model based on Directed Acyclic Graphs (DAGs) and introduces a simulation algorithm based on a novel abstraction called *event sets*. CEPSim can be used to model different types of clouds, including public, private, hybrid, and multi-cloud environments, and to simulate execution of user-defined queries on them. In addition, it can also be customized with various operator placement and scheduling strategies. These features enable architects and researchers to analyse the scalability and performance of cloud-based CEP and SP systems and to compare easily the effects of different query processing strategies.

CEPSim's theoretical background, algorithms *rationale*, and an overview of the system design can be consulted in the article:
* W. A. Higashino, Miriam A. M. Capretz, Luiz F. Bittencourt. *"CEPSim: Modelling and simulation of Complex Event Processing systems in cloud environments"*, Future Generation Computer Systems, [doi:10.1016/j.future.2015.10.023](http://dx.doi.org/10.1016/j.future.2015.10.023).
 
## Build
CEPSim's can be built using [maven](http://maven.apache.org). Just clone the repository and use the goal`install`. Note that CEPSim depends on CloudSim, which has not been mavenized and it is not available in any Maven public repository yet. Therefore, the build will automatically install CloudSim 3.0.3 at your local repository.

## Usage
In the component  `cepsim-integration`, package `ca.uwo.eng.sel.cepsim.example` there are samples that can be consulted to learn how to use CEPSim. These samples are also described in the experimental section of the article.

A lot of the work consist in creating the simulation environment using CloudSim's API. After this step, you define your queries usig CEPSim's extension and submit them for execution.

Better documentation will be provided in the next few months.
