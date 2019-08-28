# This file is the actual code for the Python runnable push-to-remote
from dataiku.runnables import Runnable
import dataikuapi
import dataiku

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def run(self, progress_callback):
        
        
        # set dimension column for each dataframe
        remote_url = get_recipe_config()['remote_url']
        remote_key = get_recipe_config()['remote_key']
        
        rc = dataikuapi.DSSClient("http://localhost:9000",api_key = "nVf4mBlWKQCDxEjlrG3tX5FqmvaUCdCD")

        cp = dataiku.api_client().get_project(dataiku.default_project_key())

        proj = rc.get_project(dataiku.default_project_key())

        remote_wiki = proj.get_wiki()

        local_wiki = cp.get_wiki()


        for r_article in remote_wiki.list_articles():
            for l_article in local_wiki.list_articles():
                if l_article.article_id == r_article.article_id:
                    l_article.delete            
                    local_wiki.create_article(article.article_id,content = article.get_data().get_body())
                    
        return '<body>Wiki Updated on instance running at: ' + remote_url + '</body>'
        