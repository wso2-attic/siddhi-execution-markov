siddhi-execution-markov
======================================

The **siddhi-execution-markov extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that allows abnormal patterns relating to user activity to be detected when carrying out real time analysis.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-markov">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-markov/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-markov/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-markov/api/4.0.14">4.0.14</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-markov/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.execution.markov</groupId>
        <artifactId>siddhi-execution-markov</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-markov/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-markov/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-markov/api/4.0.14/#markovchain-stream-processor">markovChain</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>*<br><div style="padding-left: 1em;"><p>The Markov Models extension allows abnormal patterns relating to user activity to be detected when carrying out real time analysis. There are two approaches for using this extension.1. You can input an existing Markov matrix as a csv file. It should be a N x N matrix,    and the first row should include state names.2. You can use a reasonable amount of incoming data to train a Markov matrix and then using it to   create notifications.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-markov/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-markov/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
