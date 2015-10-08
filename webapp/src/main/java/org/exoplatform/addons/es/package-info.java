/*
 * Copyright 2013 eXo Platform SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@juzu.Application(defaultController = IndexingManagementApplication.class)
@Portlet
@WebJars({
    @WebJar("jquery"),
    @WebJar("angularjs")
})
@Scripts({
    @Script(id = "jquery", value = "jquery/1.10.2/jquery.js"),
    @Script(id = "angularjs", value = "angularjs/1.4.6/angular.min.js"),
    @Script(value = "javascripts/app.js", depends = "angularjs"),
    @Script(value = "javascripts/service.js", depends = "angularjs"),
    @Script(value = "javascripts/controller.js", depends = "angularjs")
})
@Less({
    @Stylesheet(id = "indexingManagement-less", value = "styles/indexingManagement.less")
})
@Assets("*")
package org.exoplatform.addons.es;

import juzu.plugin.asset.Assets;
import juzu.plugin.asset.Script;
import juzu.plugin.asset.Scripts;
import juzu.plugin.asset.Stylesheet;
import juzu.plugin.less4j.Less;
import juzu.plugin.portlet.Portlet;
import juzu.plugin.webjars.WebJar;
import juzu.plugin.webjars.WebJars;