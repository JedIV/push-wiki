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
        
        # get remote key and url
        remote_url = self.config['remote_url']
        remote_key = self.config['remote_key']


        # get local and remote wikis for current project      
        rp = dataikuapi.DSSClient(remote_url,api_key = remote_key).get_project(self.project_key)
        cp = dataiku.api_client().get_project(self.project_key)
        
        local_wiki  = cp.get_wiki()
        remote_wiki = rp.get_wiki()

        # replace or create new articles in the project wikis
        for r_article in local_wiki.list_articles():
            for l_article in remote_wiki.list_articles():
                if l_article.article_id == r_article.article_id:
                    l_article.delete            
                remote_wiki.create_article(l_article.article_id,content = l_article.get_data().get_body())
                    
        return '<body>Wiki Updated on instance running at: ' + remote_url + '</body>'
        