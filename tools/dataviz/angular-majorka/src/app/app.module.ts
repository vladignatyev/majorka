import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { CampaignsComponent } from './campaigns/campaigns.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { NavbarComponent } from './navbar/navbar.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { UicomponentsModule } from './uicomponents/uicomponents.module';

@NgModule({
  declarations: [
    AppComponent,
    CampaignsComponent,
    DashboardComponent,
    NavbarComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    UicomponentsModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
