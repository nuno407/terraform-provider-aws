class SourceCommuter(object):
    '''Polymorphic source commuter'''
    
    def __init__(self, rules) -> None:
        '''SourceCommuter - Switches between different sources according to the number of consecutive polls allowed by the rules.

        Args:
            rules ({Any:Numeric} | [Any]): Key-Value pairs of sources and number of max consecutive polls, or list of sources. Sources must be hashable objects.
        '''
        if type(rules) == dict:
            self.sources = rules  # {source:max poll} or [source]
        else:
            self.sources = {source:1 for source in rules}
        self.index = 0  # index of the currently used source
        self.current_source = list(self.sources)[self.index]
        self.counter = self.sources.get(self.current_source)  # number of polls left for current source
    
    def get_source(self):
        """Return a source. If the counter for the current source has reached its limit, the current source gets promoted. 

        Returns:
            current_source (Any): Source to be used
        """
        # count a poll, switch if no polls left for current source
        if self.counter == 0:
            self._promote()
            self._switch()
        else:
            self.counter -= 1
        return self.current_source
    
    def next(self):
        """Force switch to another source."""
        self._demote()
        self._switch()
    
    def _switch(self) -> None:
        """Switch current_source into another source."""
        # go to next source
        self.index = (self.index + 1) % len(self.sources)
        self.current_source = list(self.sources)[self.index]
        # Reset the counter to its max poll
        self.counter = self.sources.get(self.current_source)
    
    def _promote(self) -> None:
        """Increase the number of max usages for the current source. Uses a factor of 2."""
        self.sources.update({self.current_source: self.sources.get(self.current_source)*2})
    
    def _demote(self) -> None:
        """Decrease the number of max usages for the current source. Uses a factor of 2."""
        current_max = self.sources.get(self.current_source)
        if current_max > 1:
            self.sources.update({self.current_source: current_max // 2})
