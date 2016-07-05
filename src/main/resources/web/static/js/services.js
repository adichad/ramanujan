
'use strict';

angular.module('ramanujanServices', ['ngResource']).factory('Table', function($httpProvider,$resource) {
  $httpProvider.defaults.useXDomain = true;
  delete $httpProvider.defaults.headers.common['X-Requested-With'];
  return $resource('web/static');
});